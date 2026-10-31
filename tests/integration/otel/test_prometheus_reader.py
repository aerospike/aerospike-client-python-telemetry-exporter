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

"""A live client, scraped through the webapp's Prometheus reader.

The webapp owns the Prometheus library and the scrape server. These tests
compare one published metric, ``total_nodes``, with the scrape. The second
test puts a periodic OpenTelemetry reader on the same meter provider.
"""

from __future__ import annotations

import socket
import threading
import time
from urllib.request import urlopen

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    custom_metrics_settings,
    namespace,
    seed_host,
)

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

pytestmark = pytest.mark.integration

_NODES = "aerospike.client.cluster.total_nodes"
_PROMETHEUS_NODES = "aerospike_client_cluster_total_nodes"


class _Capture(MetricExporter):
    """Records each periodic collect. The reader calls this from its thread."""

    def __init__(self) -> None:
        super().__init__()
        self._values: list[float] = []
        self._lock = threading.Lock()

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        value = _otel_value(metrics_data, _NODES)
        with self._lock:
            if value is not None:
                self._values.append(value)
        return MetricExportResult.SUCCESS

    def shutdown(self, timeout_millis=30_000, **kwargs):
        return None

    def force_flush(self, timeout_millis=10_000):
        return True

    def node_counts(self) -> list[float]:
        with self._lock:
            return list(self._values)


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


def _scrape_nodes(port: int) -> float:
    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        text = response.read().decode()
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith(_PROMETHEUS_NODES + "{") or line.startswith(_PROMETHEUS_NODES + " "):
            return float(line.rsplit(" ", 1)[-1])
    raise AssertionError(f"{_PROMETHEUS_NODES} is absent from the Prometheus scrape")


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


def _published_nodes(exporter: OtelExporter) -> float | None:
    for key in exporter.registry.client_keys():
        document = exporter.registry.current(key)
        if document is not None and isinstance(document.get("total_nodes"), (int, float)):
            return float(document["total_nodes"])
    return None


def _wait_for_publish(exporter: OtelExporter) -> float:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        published = _published_nodes(exporter)
        if published is not None:
            return published
        time.sleep(0.05)
    raise AssertionError("periodic scheduler did not publish total_nodes")


def _connect(host: str, port: int):
    return (
        ClusterDefinition(host, port)
        .app_id("prometheus-reader")
        .with_system_settings(custom_metrics_settings())
        .connect()
    )


def _exercise(cluster) -> None:
    cluster.enable_metrics(MetricsPolicy(operational_enabled=True))
    session = cluster.create_session()
    session.upsert(DataSet.of(namespace(), "prometheus_reader").id(1)).put({"n": 1}).execute()


def test_prometheus_scrape_matches_published_total_nodes():
    host, seed_port = seed_host()
    reader, port = _prometheus_reader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _exercise(cluster)
        published = _wait_for_publish(exporter)
        assert _scrape_nodes(port) == published
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()


def test_prometheus_and_otel_report_the_same_total_nodes():
    host, seed_port = seed_host()
    capture = _Capture()
    periodic = PeriodicExportingMetricReader(capture, export_interval_millis=200)
    prometheus, port = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    exporter = OtelExporter(provider)
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _exercise(cluster)
        published = _wait_for_publish(exporter)
        assert _scrape_nodes(port) == published
        deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
        while time.monotonic() < deadline and published not in capture.node_counts():
            time.sleep(0.05)
        assert published in capture.node_counts()
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()
