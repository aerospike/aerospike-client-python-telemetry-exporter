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

"""Sync acceptance tests for a real periodic metrics callback.

These tests wait on the client's export thread. They do not call timer
internals or poll ``metrics_snapshot()`` in place of ``export``.
"""

import threading
import time

import pytest

from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry import RecordingExporter
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


def _connect():
    host, port = seed_host()
    definition = (
        ClusterDefinition(host, port)
        .app_id("telemetry-exporter-itest")
        .with_system_settings(custom_metrics_settings())
    )
    try:
        return definition.connect()
    except Exception as exc:
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")


def _write(cluster) -> None:
    session = cluster.create_session()
    ds = dataset()
    for i in range(3):
        session.upsert(ds.id(i)).put({"n": i}).execute()


def _wait_until(predicate, message: str) -> None:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(message)


_EXPORT_THREAD_NAME = "aerospike-metrics-export"
_ENABLE_DISABLE_CYCLES = 4


def _live_export_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name == _EXPORT_THREAD_NAME and thread.is_alive()
    ]


def _assert_quiet(exporter, frozen: int) -> None:
    time.sleep(EXPORT_INTERVAL_SECONDS * 2.5)
    assert exporter.callbacks == frozen


class TestSyncPeriodicExport:

    def test_custom_mode_delivers_a_periodic_callback(self):
        exporter = RecordingExporter()
        with _connect() as cluster:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(metrics_policy())
            _write(cluster)
            _wait_until(
                lambda: exporter.callbacks >= 1,
                "periodic scheduler did not call export()",
            )
            record = exporter.records[0]
            assert record.client_type == "python-sdk"
            assert record.app_id == "telemetry-exporter-itest"
            assert record.node_names
            assert exporter.closed is False

    def test_two_exporters_receive_the_same_cycle(self):
        first = RecordingExporter()
        second = RecordingExporter()
        with _connect() as cluster:
            cluster.add_exporter(first)
            cluster.add_exporter(second)
            cluster.enable_metrics(metrics_policy())
            _write(cluster)
            _wait_until(
                lambda: first.callbacks >= 1 and second.callbacks >= 1,
                "both exporters were not called in one cycle",
            )
            assert_same_cycle(first.records[0], second.records[0])

    def test_one_exporter_failure_does_not_stop_the_other(self):
        failing = RecordingExporter(fail_times=100)
        healthy = RecordingExporter()
        with _connect() as cluster:
            cluster.add_exporter(failing)
            cluster.add_exporter(healthy)
            cluster.enable_metrics(metrics_policy())
            _write(cluster)
            _wait_until(
                lambda: healthy.callbacks >= 1 and failing.errors >= 1,
                "a failing exporter stopped the healthy one",
            )
            assert failing.callbacks == 0
            assert healthy.records[0].client_type == "python-sdk"

    def test_disable_stops_callbacks_without_destroying_the_exporter(self):
        exporter = RecordingExporter()
        with _connect() as cluster:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(metrics_policy())
            _write(cluster)
            _wait_until(lambda: exporter.callbacks >= 1, "no callback before disable")
            cluster.disable_metrics()
            frozen = exporter.callbacks
            assert cluster.exporters == (exporter,)
            assert exporter.closed is False
            _assert_quiet(exporter, frozen)

    def test_reenable_resumes_callbacks_on_the_same_exporter(self):
        exporter = RecordingExporter()
        with _connect() as cluster:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(metrics_policy())
            _write(cluster)
            _wait_until(lambda: exporter.callbacks >= 1, "no callback before disable")
            cluster.disable_metrics()
            frozen = exporter.callbacks
            _assert_quiet(exporter, frozen)
            assert exporter.closed is False
            cluster.enable_metrics(metrics_policy())
            _write(cluster)
            _wait_until(
                lambda: exporter.callbacks > frozen,
                "re-enable did not resume callbacks on the same exporter",
            )
            assert cluster.exporters == (exporter,)

    def test_repeated_enable_and_disable_stops_the_export_thread(self):
        exporter = RecordingExporter()
        with _connect() as cluster:
            cluster.add_exporter(exporter)
            for cycle in range(1, _ENABLE_DISABLE_CYCLES + 1):
                before = exporter.callbacks
                already_running = {thread.ident for thread in _live_export_threads()}
                cluster.enable_metrics(metrics_policy())
                assert cluster.metrics_enabled() is True
                started = [
                    thread
                    for thread in _live_export_threads()
                    if thread.ident not in already_running
                ]
                assert len(started) == 1, f"cycle {cycle} did not start one export thread"
                thread = started[0]
                _write(cluster)
                _wait_until(
                    lambda: exporter.callbacks > before,
                    f"cycle {cycle} did not call export()",
                )
                cluster.disable_metrics()
                assert cluster.metrics_enabled() is False
                assert thread.is_alive() is False
                frozen = exporter.callbacks
                assert frozen > before
                assert exporter.closed is False
                assert exporter.errors == 0
                assert cluster.exporters == (exporter,)
                _write(cluster)
                _assert_quiet(exporter, frozen)
