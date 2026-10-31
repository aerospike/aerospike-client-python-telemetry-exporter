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

"""Prometheus is another reader on the webapp's meter provider.

The webapp owns ``opentelemetry-exporter-prometheus`` and starts the scrape
server. ``OtelExporter`` only registers observable instruments. One test uses
that reader alone. The other puts it beside a periodic OpenTelemetry reader.
"""

from __future__ import annotations

import socket
import threading
import time
from urllib.request import urlopen

import pytest

from aerospike_client_telemetry.otel import OtelExporter

pytest.importorskip("opentelemetry.sdk.metrics")
pytest.importorskip("opentelemetry.exporter.prometheus")

from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExportResult,
    MetricExporter,
    PeriodicExportingMetricReader,
)
from prometheus_client import CollectorRegistry, start_http_server

_NODES = "aerospike.client.cluster.total_nodes"
_PROMETHEUS_NODES = "aerospike_client_cluster_total_nodes"
_WAIT_SECONDS = 3


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


class _Capture(MetricExporter):
    """Records each periodic collect. The reader calls this from its thread."""

    def __init__(self) -> None:
        super().__init__()
        self._batches: list = []
        self._lock = threading.Lock()

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        with self._lock:
            self._batches.append(metrics_data)
        return MetricExportResult.SUCCESS

    def shutdown(self, timeout_millis=30_000, **kwargs):
        return None

    def force_flush(self, timeout_millis=10_000):
        return True

    def node_counts(self) -> list[float]:
        with self._lock:
            batches = list(self._batches)
        counts = []
        for batch in batches:
            value = _otel_value(batch, _NODES)
            if value is not None:
                counts.append(value)
        return counts


def _document(total_nodes: int) -> _Snapshot:
    return _Snapshot(
        {
            "client_type": "python-sdk",
            "client_version": "1.0.0",
            "cluster_name": "local",
            "app_id": "orders",
            "total_nodes": total_nodes,
        }
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _prometheus_reader() -> tuple[PrometheusMetricReader, int]:
    registry = CollectorRegistry()
    reader = PrometheusMetricReader(registry=registry)
    port = _free_port()
    start_http_server(port, addr="127.0.0.1", registry=registry)
    return reader, port


def _scrape(port: int) -> str:
    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()


def _prometheus_value(text: str, name: str) -> float:
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith(name + "{") or line.startswith(name + " "):
            return float(line.rsplit(" ", 1)[-1])
    raise AssertionError(f"{name} is absent from the Prometheus scrape")


def _otel_value(metrics_data, name: str) -> float | None:
    for resource in metrics_data.resource_metrics:
        for scope in resource.scope_metrics:
            if scope.scope.name != "aerospike.client":
                continue
            for metric in scope.metrics:
                if metric.name != name:
                    continue
                points = list(metric.data.data_points)
                if points:
                    return float(points[0].value)
    return None


def _wait_for_nodes(capture: _Capture, total_nodes: float) -> None:
    deadline = time.monotonic() + _WAIT_SECONDS
    last: list[float] = []
    while time.monotonic() < deadline:
        last = capture.node_counts()
        if total_nodes in last:
            return
        time.sleep(0.02)
    raise AssertionError(f"periodic reader did not report {total_nodes} nodes: {last}")


def test_a_prometheus_reader_serves_the_published_snapshot():
    reader, port = _prometheus_reader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    try:
        exporter.export(_document(2))
        assert _prometheus_value(_scrape(port), _PROMETHEUS_NODES) == 2.0
    finally:
        provider.shutdown()


def test_prometheus_and_otel_readers_share_one_meter_provider():
    capture = _Capture()
    periodic = PeriodicExportingMetricReader(capture, export_interval_millis=200)
    prometheus, port = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    exporter = OtelExporter(provider)
    try:
        exporter.export(_document(2))
        assert _prometheus_value(_scrape(port), _PROMETHEUS_NODES) == 2.0
        _wait_for_nodes(capture, 2.0)
    finally:
        provider.shutdown()
