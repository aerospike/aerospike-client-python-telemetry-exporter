# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Async acceptance tests for a real periodic metrics callback.

These tests wait on the client's export task. They do not call timer
internals or poll ``metrics_snapshot()`` in place of ``export``.
"""

import asyncio
import time

import pytest

from aerospike_sdk import ClusterDefinition

from aerospike_client_telemetry import AsyncRecordingExporter
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    EXPORT_INTERVAL_SECONDS,
    assert_same_cycle,
    custom_metrics_settings,
    dataset,
    metrics_policy,
    seed_host,
)

pytestmark = pytest.mark.integration


def _definition():
    host, port = seed_host()
    return (
        ClusterDefinition(host, port)
        .app_id("telemetry-exporter-itest")
        .with_system_settings(custom_metrics_settings())
    )


async def _connect():
    host, port = seed_host()
    try:
        return await _definition().connect()
    except Exception as exc:
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")


async def _write(cluster) -> None:
    session = cluster.create_session()
    ds = dataset()
    for i in range(3):
        await session.upsert(ds.id(i)).put({"n": i}).execute()


async def _wait_until(predicate, message: str) -> None:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(message)


async def _settle_and_assert_quiet(exporter) -> int:
    """Wait out the closing push, then prove no further callbacks arrive."""
    deadline = time.monotonic() + 1.0
    last = exporter.callbacks
    stable_since = time.monotonic()
    while time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        current = exporter.callbacks
        if current != last:
            last = current
            stable_since = time.monotonic()
        elif time.monotonic() - stable_since >= 0.3:
            break
    frozen = exporter.callbacks
    await asyncio.sleep(EXPORT_INTERVAL_SECONDS * 2.5)
    assert exporter.callbacks == frozen
    return frozen


class TestAsyncPeriodicExport:

    async def test_custom_mode_delivers_a_periodic_callback(self):
        exporter = AsyncRecordingExporter()
        cluster = await _connect()
        try:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(metrics_policy())
            await _write(cluster)
            await _wait_until(
                lambda: exporter.callbacks >= 1,
                "periodic scheduler did not call export()",
            )
            record = exporter.records[0]
            assert record.client_type == "python-sdk"
            assert record.conn_type == "async"
            assert record.app_id == "telemetry-exporter-itest"
            assert record.node_names
            assert exporter.closed is False
        finally:
            await cluster.close()

    async def test_two_exporters_receive_the_same_cycle(self):
        first = AsyncRecordingExporter()
        second = AsyncRecordingExporter()
        cluster = await _connect()
        try:
            cluster.add_exporter(first)
            cluster.add_exporter(second)
            cluster.enable_metrics(metrics_policy())
            await _write(cluster)
            await _wait_until(
                lambda: first.callbacks >= 1 and second.callbacks >= 1,
                "both exporters were not called in one cycle",
            )
            assert_same_cycle(first.records[0], second.records[0])
        finally:
            await cluster.close()

    async def test_one_exporter_failure_does_not_stop_the_other(self):
        failing = AsyncRecordingExporter(fail_times=100)
        healthy = AsyncRecordingExporter()
        cluster = await _connect()
        try:
            cluster.add_exporter(failing)
            cluster.add_exporter(healthy)
            cluster.enable_metrics(metrics_policy())
            await _write(cluster)
            await _wait_until(
                lambda: healthy.callbacks >= 1 and failing.errors >= 1,
                "a failing exporter stopped the healthy one",
            )
            assert failing.callbacks == 0
            assert healthy.records[0].client_type == "python-sdk"
        finally:
            await cluster.close()

    async def test_disable_stops_callbacks_without_destroying_the_exporter(self):
        exporter = AsyncRecordingExporter()
        cluster = await _connect()
        try:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(metrics_policy())
            await _write(cluster)
            await _wait_until(lambda: exporter.callbacks >= 1, "no callback before disable")
            cluster.disable_metrics()
            frozen = await _settle_and_assert_quiet(exporter)
            assert cluster.exporters == (exporter,)
            assert exporter.closed is False
            assert frozen >= 1
        finally:
            await cluster.close()

    async def test_reenable_resumes_callbacks_on_the_same_exporter(self):
        exporter = AsyncRecordingExporter()
        cluster = await _connect()
        try:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(metrics_policy())
            await _write(cluster)
            await _wait_until(lambda: exporter.callbacks >= 1, "no callback before disable")
            cluster.disable_metrics()
            frozen = await _settle_and_assert_quiet(exporter)
            assert exporter.closed is False
            cluster.enable_metrics(metrics_policy())
            await _write(cluster)
            await _wait_until(
                lambda: exporter.callbacks > frozen,
                "re-enable did not resume callbacks on the same exporter",
            )
            assert cluster.exporters == (exporter,)
        finally:
            await cluster.close()
