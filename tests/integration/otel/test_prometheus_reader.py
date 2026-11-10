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

"""A live client, scraped through the Prometheus reader.

One test uses only ``CustomPrometheusMetricReader`` and the exporter's HTTP
server. The webapp tests serve that registry with the webapp's own HTTP
server, in plaintext, TLS, and mutual TLS. The other tests put a periodic
OpenTelemetry reader on the same meter provider.

A webapp meter and ``OtelExporter`` share that provider. A periodic reader
and a Prometheus reader each collect both families. A second periodic reader
registered later collects the same families. The Prometheus scrape is that
collection, not a merge of the periodic exports. One test compares this
reader with the pinned OpenTelemetry Prometheus reader while operational
metrics are disabled and again while they are enabled. Certificates are
minted in a temporary directory and are not checked in.
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

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    active_node_count,
    command_count,
    custom_metrics_settings,
    namespace,
    seed_host,
)

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
pytestmark = pytest.mark.integration

_NODES = "aerospike_client_nodes_active"
_PROMETHEUS_NODES = "aerospike_client_nodes_active"


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
            if scope.scope.name != "aerospike_client":
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
        nodes = None if document is None else active_node_count(document)
        if isinstance(nodes, (int, float)) and not isinstance(nodes, bool):
            return float(nodes)
    return None


def _wait_for_publish(exporter: OtelExporter) -> float:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        published = _published_nodes(exporter)
        if published is not None:
            return published
        time.sleep(0.05)
    raise AssertionError("periodic scheduler did not publish nodes_active")


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


def _record_webapp(placed, backlog) -> None:
    placed.add(2)
    backlog.set(4)


def _scrape_text(port: int, context: ssl.SSLContext | None = None) -> str:
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
    return _scrape_text(port, context=tls_client(client_cert))


def _prometheus_value(text: str, name: str) -> float:
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith(name + "{") or line.startswith(name + " "):
            return float(line.rsplit(" ", 1)[-1])
    raise AssertionError(f"{name} is absent from the Prometheus scrape")


def _wait_for_series(capture: _SeriesCapture, scope: str, name: str, predicate) -> float:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        last = capture.value(scope, name)
        if last is not None and predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"{scope}/{name} is {last}")


def _assert_shared_series(capture: _SeriesCapture, published_nodes: float) -> None:
    assert _wait_for_series(capture, "orders", _ORDERS_PLACED, lambda value: value == 2) == 2
    assert _wait_for_series(capture, "orders", _ORDERS_BACKLOG, lambda value: value == 4) == 4
    nodes = _wait_for_series(
        capture, "aerospike_client", _NODES, lambda value: value == published_nodes
    )
    assert nodes == published_nodes


def _assert_prometheus_mix(port: int, published_nodes: float) -> None:
    text = _scrape_text(port)
    assert _prometheus_value(text, _PROMETHEUS_PLACED) == 2
    assert _prometheus_value(text, _PROMETHEUS_BACKLOG) == 4
    assert _prometheus_value(text, _PROMETHEUS_NODES) == published_nodes


def test_prometheus_scrape_matches_published_nodes_active():
    """Only the Prometheus reader. The live client series is served on its HTTP port."""
    host, seed_port = seed_host()
    reader = _prometheus_reader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    port = _serve(exporter)
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


def test_prometheus_and_otel_report_the_same_nodes_active():
    host, seed_port = seed_host()
    capture = _Capture()
    periodic = PeriodicExportingMetricReader(capture, export_interval_millis=200)
    prometheus = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    exporter = OtelExporter(provider)
    port = _serve(exporter)
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


def test_periodic_and_prometheus_readers_report_webapp_and_client_metrics():
    """One provider: webapp instruments and the client exporter, two readers."""
    host, seed_port = seed_host()
    capture = _SeriesCapture()
    periodic = PeriodicExportingMetricReader(capture, export_interval_millis=200)
    prometheus = _prometheus_reader()
    provider = MeterProvider(metric_readers=[periodic, prometheus])
    placed, backlog = _webapp_instruments(provider)
    exporter = OtelExporter(provider)
    port = _serve(exporter)
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        _record_webapp(placed, backlog)
        cluster.add_exporter(exporter)
        _exercise(cluster)
        published = _wait_for_publish(exporter)
        _assert_shared_series(capture, published)
        _assert_prometheus_mix(port, published)
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()


def test_two_periodic_readers_and_prometheus_report_the_same_metrics():
    """A later periodic reader collects the same series the first reader and Prometheus do."""
    host, seed_port = seed_host()
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
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        _record_webapp(placed, backlog)
        cluster.add_exporter(exporter)
        _exercise(cluster)
        published = _wait_for_publish(exporter)
        _assert_shared_series(first, published)
        _assert_shared_series(second, published)
        _assert_prometheus_mix(port, published)
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()


def test_webapp_http_server_serves_published_client_metrics():
    """The webapp serves the reader registry. No periodic reader and no exporter server."""
    host, seed_port = seed_host()
    registry = CollectorRegistry()
    reader = CustomPrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    httpd, thread, port = _webapp_http(registry)
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        _stop_webapp(httpd, thread)
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _exercise(cluster)
        published = _wait_for_publish(exporter)
        assert _scrape_nodes(port) == published
        provider.shutdown()
        assert _PROMETHEUS_NODES not in _scrape_text(port)
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()
        _stop_webapp(httpd, thread)


def test_webapp_http_server_appends_published_client_metrics_to_its_own():
    """Webapp metrics stay on its registry, and the reader adds the live client series."""
    host, seed_port = seed_host()
    registry = CollectorRegistry()
    Counter("orders_placed", "Orders placed", registry=registry).inc(2)
    reader = CustomPrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[reader])
    backlog = provider.get_meter("orders").create_gauge("orders_backlog")
    exporter = OtelExporter(provider)
    httpd, thread, port = _webapp_http(registry)
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        _stop_webapp(httpd, thread)
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        backlog.set(4)
        cluster.add_exporter(exporter)
        _exercise(cluster)
        published = _wait_for_publish(exporter)
        text = _scrape_text(port)
        assert _prometheus_value(text, "orders_placed_total") == 2
        assert _prometheus_value(text, "orders_backlog") == 4
        assert _prometheus_value(text, _PROMETHEUS_NODES) == published
        provider.shutdown()
        text = _scrape_text(port)
        assert _prometheus_value(text, "orders_placed_total") == 2
        assert _PROMETHEUS_NODES not in text
        assert "orders_backlog" not in text
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()
        _stop_webapp(httpd, thread)


def _connect_or_skip(provider, httpd, thread, host: str, seed_port: int):
    try:
        return _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        _stop_webapp(httpd, thread)
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")


@needs_openssl
def test_webapp_http_server_serves_published_client_metrics_over_tls():
    """The webapp's TLS server serves the live client series. No client certificate."""
    host, seed_port = seed_host()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        registry = CollectorRegistry()
        reader = CustomPrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])
        exporter = OtelExporter(provider)
        httpd, thread, port = _webapp_tls(registry, root, mutual=False)
        cluster = _connect_or_skip(provider, httpd, thread, host, seed_port)
        try:
            cluster.add_exporter(exporter)
            _exercise(cluster)
            published = _wait_for_publish(exporter)
            with pytest.raises((URLError, ssl.SSLError, ConnectionResetError)):
                _scrape_text(port)
            assert _prometheus_value(_scrape_tls(port, root, mutual=False), _PROMETHEUS_NODES) == published
            provider.shutdown()
            assert _PROMETHEUS_NODES not in _scrape_tls(port, root, mutual=False)
        finally:
            cluster.disable_metrics()
            cluster.close()
            provider.shutdown()
            _stop_webapp(httpd, thread)


@needs_openssl
def test_webapp_http_server_serves_published_client_metrics_over_mutual_tls():
    """The webapp's mutual TLS server requires a client certificate for the live series."""
    host, seed_port = seed_host()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        registry = CollectorRegistry()
        reader = CustomPrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])
        exporter = OtelExporter(provider)
        httpd, thread, port = _webapp_tls(registry, root, mutual=True)
        cluster = _connect_or_skip(provider, httpd, thread, host, seed_port)
        try:
            cluster.add_exporter(exporter)
            _exercise(cluster)
            published = _wait_for_publish(exporter)
            with pytest.raises((URLError, ssl.SSLError)):
                _scrape_text(port, context=tls_client())
            text = _scrape_tls(port, root, mutual=True)
            assert _prometheus_value(text, _PROMETHEUS_NODES) == published
            provider.shutdown()
            assert _PROMETHEUS_NODES not in _scrape_tls(port, root, mutual=True)
        finally:
            cluster.disable_metrics()
            cluster.close()
            provider.shutdown()
            _stop_webapp(httpd, thread)


@needs_openssl
def test_webapp_http_server_appends_published_client_metrics_over_tls():
    """Webapp series and the live client series share the webapp's TLS scrape."""
    host, seed_port = seed_host()
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
        cluster = _connect_or_skip(provider, httpd, thread, host, seed_port)
        try:
            backlog.set(4)
            cluster.add_exporter(exporter)
            _exercise(cluster)
            published = _wait_for_publish(exporter)
            text = _scrape_tls(port, root, mutual=False)
            assert _prometheus_value(text, "orders_placed_total") == 2
            assert _prometheus_value(text, "orders_backlog") == 4
            assert _prometheus_value(text, _PROMETHEUS_NODES) == published
            provider.shutdown()
            text = _scrape_tls(port, root, mutual=False)
            assert _prometheus_value(text, "orders_placed_total") == 2
            assert _PROMETHEUS_NODES not in text
            assert "orders_backlog" not in text
        finally:
            cluster.disable_metrics()
            cluster.close()
            provider.shutdown()
            _stop_webapp(httpd, thread)


@needs_openssl
def test_webapp_http_server_appends_published_client_metrics_over_mutual_tls():
    """Webapp series and the live client series share the webapp's mutual TLS scrape."""
    host, seed_port = seed_host()
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
        cluster = _connect_or_skip(provider, httpd, thread, host, seed_port)
        try:
            backlog.set(4)
            cluster.add_exporter(exporter)
            _exercise(cluster)
            published = _wait_for_publish(exporter)
            with pytest.raises((URLError, ssl.SSLError)):
                _scrape_text(port, context=tls_client())
            text = _scrape_tls(port, root, mutual=True)
            assert _prometheus_value(text, "orders_placed_total") == 2
            assert _prometheus_value(text, "orders_backlog") == 4
            assert _prometheus_value(text, _PROMETHEUS_NODES) == published
            provider.shutdown()
            text = _scrape_tls(port, root, mutual=True)
            assert _prometheus_value(text, "orders_placed_total") == 2
            assert _PROMETHEUS_NODES not in text
            assert "orders_backlog" not in text
        finally:
            cluster.disable_metrics()
            cluster.close()
            provider.shutdown()
            _stop_webapp(httpd, thread)


def _wait_for_operational(exporter: OtelExporter, enabled: bool) -> dict:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        for key in exporter.registry.client_keys():
            document = exporter.registry.current(key)
            if not isinstance(document, dict):
                continue
            last = document
            flag = document.get("operational_metrics_enabled") is True
            if (
                flag is enabled
                and isinstance(active_node_count(document), (int, float))
                and isinstance(command_count(document), (int, float))
            ):
                return document
        time.sleep(0.05)
    raise AssertionError(f"operational_metrics_enabled={enabled} was not published: {last}")


def _type_line(text: str, name: str, kind: str) -> bool:
    return any(
        line.startswith("# TYPE ") and name in line and line.endswith(" " + kind)
        for line in text.splitlines()
    )


def test_custom_reader_matches_official_reader_for_enabled_and_disabled_metrics():
    """Live gauges and counters match the pinned reader in both operational modes."""
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from prometheus_client import generate_latest

    host, seed_port = seed_host()
    custom_registry = CollectorRegistry()
    official_registry = CollectorRegistry()
    custom = CustomPrometheusMetricReader(registry=custom_registry)
    official = PrometheusMetricReader(registry=official_registry)
    provider = MeterProvider(metric_readers=[custom, official])
    exporter = OtelExporter(provider)
    cluster = None
    try:
        cluster = _connect(host, seed_port)
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{seed_port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        session = cluster.create_session()
        for operational in (False, True):
            cluster.enable_metrics(MetricsPolicy(operational_enabled=operational))
            session.upsert(DataSet.of(namespace(), "prometheus_reader").id(1)).put({"n": 1}).execute()
            document = _wait_for_operational(exporter, operational)
            cluster.disable_metrics()
            custom_text = generate_latest(custom_registry).decode()
            official_text = generate_latest(official_registry).decode()
            assert custom_text.splitlines() == official_text.splitlines()
            assert _type_line(custom_text, "nodes_active", "gauge")
            assert _type_line(custom_text, "invalid_node_count", "counter")
            if operational:
                assert document.get("operational_metrics_enabled") is True
                assert "cpu_percent" in custom_text
            else:
                assert "cpu_percent" not in custom_text
    finally:
        cluster.disable_metrics()
        cluster.close()
        provider.shutdown()
