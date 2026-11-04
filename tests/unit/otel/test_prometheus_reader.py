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

``OtelExporter`` only registers observable instruments. One test uses only
``CustomPrometheusMetricReader`` and the exporter's HTTP server. The webapp
tests serve that reader's registry with the webapp's own HTTP server, in
plaintext, TLS, and mutual TLS. The other tests put the reader beside a
periodic OpenTelemetry reader.

The webapp's own instruments share that provider. A periodic reader and a
Prometheus reader each collect both families. A second periodic reader
registered later collects the same families. Certificates are minted in a
temporary directory and are not checked in.
"""

from __future__ import annotations

import socket
import ssl
import tempfile
import threading
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from aerospike_client_telemetry.otel import OtelExporter

pytest.importorskip("opentelemetry.sdk.metrics")
pytest.importorskip("prometheus_client")

from aerospike_client_telemetry.otel.prometheus_reader import (
    CollectorRegistry,
    CustomPrometheusMetricReader,
)
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import Counter, start_http_server
from tests.prometheus_certs import mint_certificates, needs_openssl, tls_client
from opentelemetry.sdk.metrics.export import (
    MetricExportResult,
    MetricExporter,
    PeriodicExportingMetricReader,
)

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


def _prometheus_reader() -> CustomPrometheusMetricReader:
    return CustomPrometheusMetricReader(registry=CollectorRegistry())


def _serve(exporter: OtelExporter) -> int:
    return exporter.start_http_server(host="127.0.0.1", port=0).port


def _webapp_http(
    registry: CollectorRegistry,
    *,
    certfile: str | None = None,
    keyfile: str | None = None,
    client_cafile: str | None = None,
    client_auth_required: bool = False,
):
    """The webapp's own Prometheus server. This package does not own it."""
    tls = {}
    if certfile is not None:
        tls = {
            "certfile": certfile,
            "keyfile": keyfile,
            "client_cafile": client_cafile,
            "client_auth_required": client_auth_required,
            "tls_min_version": ssl.TLSVersion.TLSv1_2,
            "tls_max_version": ssl.TLSVersion.TLSv1_3,
        }
    httpd, thread = start_http_server(0, addr="127.0.0.1", registry=registry, **tls)
    port = int(httpd.server_address[1])
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return httpd, thread, port
        except OSError:
            time.sleep(0.01)
    httpd.shutdown()
    thread.join(1)
    raise AssertionError("webapp Prometheus server did not accept connections")


def _stop_webapp(httpd, thread) -> None:
    httpd.shutdown()
    httpd.server_close()
    thread.join(5)


def _scrape(port: int, context: ssl.SSLContext | None = None) -> str:
    scheme = "https" if context is not None else "http"
    with urlopen(f"{scheme}://127.0.0.1:{port}/metrics", timeout=5, context=context) as response:
        return response.read().decode()


def _webapp_tls(registry: CollectorRegistry, root: Path, *, mutual: bool):
    return _webapp_http(
        registry,
        certfile=str(root / "server.crt"),
        keyfile=str(root / "server.key"),
        client_cafile=str(root / "ca.crt") if mutual else None,
        client_auth_required=mutual,
    )


def _scrape_tls(port: int, root: Path, *, mutual: bool) -> str:
    client_cert = root / "client.crt" if mutual else None
    return _scrape(port, context=tls_client(client_cert))


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
    """Only the Prometheus reader. Client series are served on its HTTP port."""
    reader = _prometheus_reader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    port = _serve(exporter)
    try:
        exporter.export(_document(2))
        assert _prometheus_value(_scrape(port), _PROMETHEUS_NODES) == 2.0
    finally:
        provider.shutdown()


def test_webapp_http_server_serves_client_metrics_from_the_reader():
    """The webapp serves the reader registry. No periodic reader and no exporter server."""
    registry = CollectorRegistry()
    reader = CustomPrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    httpd, thread, port = _webapp_http(registry)
    try:
        exporter.export(_document(2))
        assert _prometheus_value(_scrape(port), _PROMETHEUS_NODES) == 2.0
        provider.shutdown()
        assert _PROMETHEUS_NODES not in _scrape(port)
    finally:
        provider.shutdown()
        _stop_webapp(httpd, thread)


def test_webapp_http_server_appends_client_metrics_to_its_own():
    """Webapp metrics stay on its registry, and the reader adds the client series."""
    registry = CollectorRegistry()
    Counter("orders_placed", "Orders placed", registry=registry).inc(2)
    reader = CustomPrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[reader])
    backlog = provider.get_meter("orders").create_gauge("orders_backlog")
    exporter = OtelExporter(provider)
    httpd, thread, port = _webapp_http(registry)
    try:
        backlog.set(4)
        exporter.export(_document(3))
        text = _scrape(port)
        assert _prometheus_value(text, "orders_placed_total") == 2.0
        assert _prometheus_value(text, "orders_backlog") == 4.0
        assert _prometheus_value(text, _PROMETHEUS_NODES) == 3.0
        provider.shutdown()
        text = _scrape(port)
        assert _prometheus_value(text, "orders_placed_total") == 2.0
        assert _PROMETHEUS_NODES not in text
        assert "orders_backlog" not in text
    finally:
        provider.shutdown()
        _stop_webapp(httpd, thread)


def _reject_plaintext(port: int) -> None:
    with pytest.raises((URLError, ssl.SSLError, ConnectionResetError)):
        _scrape(port)


@needs_openssl
def test_webapp_http_server_serves_client_metrics_over_tls():
    """The webapp's TLS server serves the reader. A client certificate is not required."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        registry = CollectorRegistry()
        reader = CustomPrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])
        exporter = OtelExporter(provider)
        httpd, thread, port = _webapp_tls(registry, root, mutual=False)
        try:
            exporter.export(_document(2))
            _reject_plaintext(port)
            assert _prometheus_value(_scrape_tls(port, root, mutual=False), _PROMETHEUS_NODES) == 2.0
            provider.shutdown()
            assert _PROMETHEUS_NODES not in _scrape_tls(port, root, mutual=False)
        finally:
            provider.shutdown()
            _stop_webapp(httpd, thread)


@needs_openssl
def test_webapp_http_server_serves_client_metrics_over_mutual_tls():
    """The webapp's mutual TLS server requires a client certificate."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        registry = CollectorRegistry()
        reader = CustomPrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])
        exporter = OtelExporter(provider)
        httpd, thread, port = _webapp_tls(registry, root, mutual=True)
        try:
            exporter.export(_document(2))
            _reject_plaintext(port)
            with pytest.raises((URLError, ssl.SSLError)):
                _scrape(port, context=tls_client())
            assert _prometheus_value(_scrape_tls(port, root, mutual=True), _PROMETHEUS_NODES) == 2.0
            provider.shutdown()
            assert _PROMETHEUS_NODES not in _scrape_tls(port, root, mutual=True)
        finally:
            provider.shutdown()
            _stop_webapp(httpd, thread)


@needs_openssl
def test_webapp_http_server_appends_client_metrics_over_tls():
    """Webapp series and client series share the webapp's TLS scrape."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        registry = CollectorRegistry()
        Counter("orders_placed", "Orders placed", registry=registry).inc(2)
        reader = CustomPrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])
        backlog = provider.get_meter("orders").create_gauge("orders_backlog")
        exporter = OtelExporter(provider)
        httpd, thread, port = _webapp_tls(registry, root, mutual=False)
        try:
            backlog.set(4)
            exporter.export(_document(3))
            _reject_plaintext(port)
            text = _scrape_tls(port, root, mutual=False)
            assert _prometheus_value(text, "orders_placed_total") == 2.0
            assert _prometheus_value(text, "orders_backlog") == 4.0
            assert _prometheus_value(text, _PROMETHEUS_NODES) == 3.0
            provider.shutdown()
            text = _scrape_tls(port, root, mutual=False)
            assert _prometheus_value(text, "orders_placed_total") == 2.0
            assert _PROMETHEUS_NODES not in text
            assert "orders_backlog" not in text
        finally:
            provider.shutdown()
            _stop_webapp(httpd, thread)


@needs_openssl
def test_webapp_http_server_appends_client_metrics_over_mutual_tls():
    """Webapp series and client series share the webapp's mutual TLS scrape."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        registry = CollectorRegistry()
        Counter("orders_placed", "Orders placed", registry=registry).inc(2)
        reader = CustomPrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])
        backlog = provider.get_meter("orders").create_gauge("orders_backlog")
        exporter = OtelExporter(provider)
        httpd, thread, port = _webapp_tls(registry, root, mutual=True)
        try:
            backlog.set(4)
            exporter.export(_document(3))
            with pytest.raises((URLError, ssl.SSLError)):
                _scrape(port, context=tls_client())
            text = _scrape_tls(port, root, mutual=True)
            assert _prometheus_value(text, "orders_placed_total") == 2.0
            assert _prometheus_value(text, "orders_backlog") == 4.0
            assert _prometheus_value(text, _PROMETHEUS_NODES) == 3.0
            provider.shutdown()
            text = _scrape_tls(port, root, mutual=True)
            assert _prometheus_value(text, "orders_placed_total") == 2.0
            assert _PROMETHEUS_NODES not in text
            assert "orders_backlog" not in text
        finally:
            provider.shutdown()
            _stop_webapp(httpd, thread)


def test_prometheus_and_otel_readers_share_one_meter_provider():
    capture = _Capture()
    periodic = PeriodicExportingMetricReader(capture, export_interval_millis=200)
    prometheus = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    exporter = OtelExporter(provider)
    port = _serve(exporter)
    try:
        exporter.export(_document(2))
        assert _prometheus_value(_scrape(port), _PROMETHEUS_NODES) == 2.0
        _wait_for_nodes(capture, 2.0)
    finally:
        provider.shutdown()


_ORDERS_PLACED = "orders_placed"
_ORDERS_BACKLOG = "orders_backlog"
_PROMETHEUS_PLACED = "orders_placed_total"
_PROMETHEUS_BACKLOG = "orders_backlog"


class _SeriesCapture(MetricExporter):
    """Latest value of each ``(scope, name)`` a periodic reader exports."""

    def __init__(self) -> None:
        super().__init__()
        self._values: dict[tuple[str, str], float] = {}
        self._lock = threading.Lock()

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        found: dict[tuple[str, str], float] = {}
        for resource in metrics_data.resource_metrics:
            for scope in resource.scope_metrics:
                scope_name = scope.scope.name
                for metric in scope.metrics:
                    points = list(metric.data.data_points)
                    if points:
                        found[(scope_name, metric.name)] = float(points[0].value)
        with self._lock:
            self._values.update(found)
        return MetricExportResult.SUCCESS

    def shutdown(self, timeout_millis=30_000, **kwargs):
        return None

    def force_flush(self, timeout_millis=10_000):
        return True

    def value(self, scope: str, name: str) -> float | None:
        with self._lock:
            return self._values.get((scope, name))


def _webapp_instruments(provider: MeterProvider):
    orders = provider.get_meter("orders")
    return orders.create_counter(_ORDERS_PLACED), orders.create_gauge(_ORDERS_BACKLOG)


def _wait_for_series(capture: _SeriesCapture, scope: str, name: str, expected: float) -> None:
    deadline = time.monotonic() + _WAIT_SECONDS
    last = None
    while time.monotonic() < deadline:
        last = capture.value(scope, name)
        if last == expected:
            return
        time.sleep(0.02)
    raise AssertionError(f"{scope}/{name} is {last}, expected {expected}")


def _assert_shared_series(capture: _SeriesCapture) -> None:
    _wait_for_series(capture, "orders", _ORDERS_PLACED, 2.0)
    _wait_for_series(capture, "orders", _ORDERS_BACKLOG, 4.0)
    _wait_for_series(capture, "aerospike.client", _NODES, 2.0)


def _assert_prometheus_mix(port: int) -> None:
    text = _scrape(port)
    assert _prometheus_value(text, _PROMETHEUS_PLACED) == 2.0
    assert _prometheus_value(text, _PROMETHEUS_BACKLOG) == 4.0
    assert _prometheus_value(text, _PROMETHEUS_NODES) == 2.0


def test_periodic_and_prometheus_readers_report_webapp_and_client_metrics():
    """One provider: webapp instruments and the client exporter, two readers."""
    capture = _SeriesCapture()
    periodic = PeriodicExportingMetricReader(capture, export_interval_millis=200)
    prometheus = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    placed, backlog = _webapp_instruments(provider)
    exporter = OtelExporter(provider)
    port = _serve(exporter)
    try:
        placed.add(2)
        backlog.set(4)
        exporter.export(_document(2))
        _assert_shared_series(capture)
        _assert_prometheus_mix(port)
    finally:
        provider.shutdown()


def test_two_periodic_readers_and_prometheus_report_the_same_metrics():
    """A later periodic reader collects the same series the first reader and Prometheus do."""
    first = _SeriesCapture()
    second = _SeriesCapture()
    periodic = PeriodicExportingMetricReader(first, export_interval_millis=200)
    prometheus = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    placed, backlog = _webapp_instruments(provider)
    exporter = OtelExporter(provider)
    port = _serve(exporter)
    provider.add_metric_reader(
        PeriodicExportingMetricReader(second, export_interval_millis=200)
    )
    try:
        placed.add(2)
        backlog.set(4)
        exporter.export(_document(2))
        _assert_shared_series(first)
        _assert_shared_series(second)
        _assert_prometheus_mix(port)
    finally:
        provider.shutdown()
