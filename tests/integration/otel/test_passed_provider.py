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

"""A webapp meter provider across enable and disable.

``OtelExporter(meter_provider=...)`` registers instruments on that provider.
It does not start a reader and it does not open OTLP. ``enable_metrics`` and
``disable_metrics`` start and stop the Aerospike export thread. The provider
keeps reporting the last document while that thread is stopped.
"""

from __future__ import annotations

import threading
import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.sync import ClusterDefinition
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from aerospike_client_telemetry.otel import OtelExporter, OtelExporterConfig
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    EXPORT_INTERVAL_SECONDS,
    custom_metrics_settings,
    metrics_policy,
    namespace,
    seed_host,
)

pytestmark = pytest.mark.integration

_EXPORT_THREAD_NAME = "aerospike-metrics-export"


def _reject_otlp(self, *args, **kwargs):
    raise AssertionError("a passed-in meter provider opened an OTLP connection")


def _live_export_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name == _EXPORT_THREAD_NAME and thread.is_alive()
    ]


def _metric_value(reader: InMemoryMetricReader, name: str) -> float | None:
    data = reader.get_metrics_data()
    if data is None:
        return None
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name != name:
                    continue
                points = list(metric.data.data_points)
                if not points:
                    return None
                return float(points[0].value)
    return None


def _wait_for_value(reader: InMemoryMetricReader, predicate) -> float:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        last = _metric_value(reader, "aerospike_client_command_count")
        if last is not None and predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"aerospike_client_command_count is {last}")


def _write(cluster, dataset) -> None:
    session = cluster.create_session()
    for i in range(3):
        session.upsert(dataset.id(i)).put({"n": i}).execute()


def test_passed_meter_provider_survives_enable_and_disable(monkeypatch):
    http = pytest.importorskip("opentelemetry.exporter.otlp.proto.http.metric_exporter")
    grpc = pytest.importorskip("opentelemetry.exporter.otlp.proto.grpc.metric_exporter")
    monkeypatch.setattr(http.OTLPMetricExporter, "__init__", _reject_otlp)
    monkeypatch.setattr(grpc.OTLPMetricExporter, "__init__", _reject_otlp)

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(
        OtelExporterConfig(endpoint="http://127.0.0.1:4318/v1/metrics"),
        meter_provider=provider,
    )
    assert exporter.owns_provider is False
    assert exporter.meter_provider is provider
    assert list(provider._metric_readers) == [reader]

    host, port = seed_host()
    definition = (
        ClusterDefinition(host, port)
        .app_id("passed-provider")
        .with_system_settings(custom_metrics_settings())
    )
    cluster = None
    try:
        cluster = definition.connect()
    except Exception as exc:
        exporter.close()
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    dataset = DataSet.of(namespace(), "passed_provider")
    try:
        cluster.add_exporter(exporter)
        already_running = {thread.ident for thread in _live_export_threads()}
        cluster.enable_metrics(metrics_policy())
        started = [
            thread for thread in _live_export_threads() if thread.ident not in already_running
        ]
        assert len(started) == 1
        _write(cluster, dataset)
        first = _wait_for_value(reader, lambda value: value >= 1)

        cluster.disable_metrics()
        assert cluster.metrics_enabled() is False
        assert started[0].is_alive() is False
        held = _metric_value(reader, "aerospike_client_command_count")
        assert held == first
        _write(cluster, dataset)
        time.sleep(EXPORT_INTERVAL_SECONDS * 2.5)
        assert _metric_value(reader, "aerospike_client_command_count") == held
        assert provider._shutdown is False

        cluster.enable_metrics(metrics_policy())
        assert cluster.metrics_enabled() is True
        _write(cluster, dataset)
        second = _wait_for_value(reader, lambda value: value > held)
        cluster.disable_metrics()
        assert _metric_value(reader, "aerospike_client_command_count") == second
        cluster.close()
        cluster = None

        exporter.close()
        assert provider._shutdown is False
        assert _metric_value(reader, "aerospike_client_command_count") == second
    finally:
        if cluster is not None:
            cluster.close()
        provider.shutdown()
