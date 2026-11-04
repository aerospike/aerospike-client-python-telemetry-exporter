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

"""Reader conversion, exporter HTTP ownership, TLS, and shutdown."""

from __future__ import annotations

import logging
import multiprocessing
import signal
import socket
import ssl
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
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
    PrometheusConfig,
    PrometheusHTTPServer,
    TLSConfig,
)
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import Counter, MeterProvider, ObservableCounter
from opentelemetry.sdk.metrics.export import AggregationTemporality, ExponentialHistogram, Metric
from opentelemetry.sdk.resources import Resource
from prometheus_client import generate_latest
from tests.prometheus_certs import mint_certificates, needs_openssl, tls_client


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


def _value(text: str, name: str) -> float:
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith(name + "{") or line.startswith(name + " "):
            return float(line.rsplit(" ", 1)[-1])
    raise AssertionError(f"{name} is absent from the Prometheus scrape")


def _scrape(port: int, context: ssl.SSLContext | None = None) -> str:
    scheme = "https" if context else "http"
    with urlopen(f"{scheme}://127.0.0.1:{port}/metrics", timeout=5, context=context) as response:
        return response.read().decode()


def _closed(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return False
    except OSError:
        return True


def _pair(port: int = 0):
    reader = CustomPrometheusMetricReader(registry=CollectorRegistry())
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    server = exporter.start_http_server(host="127.0.0.1", port=port)
    return reader, server, provider, exporter


def test_reader_module_does_not_name_aerospike_metrics():
    import aerospike_client_telemetry.otel.prometheus_reader as prometheus_reader

    source = Path(prometheus_reader.__file__).read_text()
    assert "total_nodes" not in source
    assert "aerospike.client" not in source
    assert "signal.signal" not in source


def test_reader_requests_cumulative_temporality_and_preserves_series():
    registry = CollectorRegistry()
    reader = CustomPrometheusMetricReader(
        prefix="custom",
        registry=registry,
        resource_attribute_filter=lambda name: name == "service.name",
    )
    assert reader._preferred_temporality[Counter] is AggregationTemporality.CUMULATIVE
    assert reader._preferred_temporality[ObservableCounter] is AggregationTemporality.CUMULATIVE
    provider = MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"service.name": "orders"}),
    )
    meter = provider.get_meter("web.app", "1.2.3")
    counter = meter.create_counter(
        "requests.processed", unit="By", description="Processed requests"
    )
    gauge = meter.create_gauge("queue.depth", description="Current queue depth")
    signed = meter.create_up_down_counter("queue.signed", description="Signed depth")
    meter.create_observable_gauge("idle", callbacks=[lambda options: []])
    try:
        counter.add(2, {"http.method": "GET", "attempt": 1})
        counter.add(3, {"http.method": "POST"})
        gauge.set(4)
        signed.add(3)
        signed.add(-1)
        text = generate_latest(registry).decode()
        assert "# HELP custom_requests_processed_bytes_total Processed requests" in text
        assert "# TYPE custom_requests_processed_bytes_total counter" in text
        assert 'http_method="GET"' in text
        assert 'attempt="1"' in text
        assert 'http_method="POST"' in text
        assert 'otel_scope_name="web.app"' in text
        assert _value(text, "custom_requests_processed_bytes_total") == 2
        assert "# TYPE custom_queue_depth gauge" in text
        assert _value(text, "custom_queue_depth") == 4
        assert "# TYPE custom_queue_signed gauge" in text
        assert "custom_queue_signed_total" not in text
        assert _value(text, "custom_queue_signed") == 2
        assert not any(
            line.startswith("custom_idle") and not line.startswith("#") for line in text.splitlines()
        )
    finally:
        provider.shutdown()


def test_unsupported_metric_is_not_exported_as_a_gauge(caplog):
    registry = CollectorRegistry()
    reader = CustomPrometheusMetricReader(registry=registry)
    metric = Metric(
        "exp.histogram",
        "unsupported",
        "1",
        ExponentialHistogram([], AggregationTemporality.CUMULATIVE),
    )
    families: dict = {}
    with caplog.at_level(logging.WARNING):
        reader._collector._translate_metric(families, metric, {}, {})
    assert families == {}
    assert "ExponentialHistogram" in caplog.text
    reader.shutdown()


def test_provider_shutdown_stops_the_server_the_exporter_started():
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)
    reader, server, provider, _exporter = _pair()
    port = server.port
    provider.get_meter("orders").create_counter("placed").add(2)
    try:
        assert _value(_scrape(port), "placed_total") == 2
        assert signal.getsignal(signal.SIGTERM) is before_term
        assert signal.getsignal(signal.SIGINT) is before_int
        assert server.registry is reader.registry
    finally:
        provider.shutdown()
    server.shutdown()
    assert _closed(port)


def test_start_http_server_requires_one_prometheus_reader():
    provider = MeterProvider(metric_readers=[])
    exporter = OtelExporter(provider)
    with pytest.raises(RuntimeError, match="CustomPrometheusMetricReader"):
        exporter.start_http_server()
    first = CustomPrometheusMetricReader()
    second = CustomPrometheusMetricReader()
    provider = MeterProvider(metric_readers=[first, second])
    exporter = OtelExporter(provider)
    with pytest.raises(RuntimeError, match="more than one"):
        exporter.start_http_server()
    server = exporter.start_http_server(host="127.0.0.1", port=0, reader=first)
    try:
        assert server.registry is first.registry
        with pytest.raises(RuntimeError, match="already started"):
            exporter.start_http_server(host="127.0.0.1", port=0, reader=second)
    finally:
        provider.shutdown()


def test_bind_failure_does_not_stop_the_server_that_already_holds_the_port():
    _held, held_server, held_provider, _held_exporter = _pair()
    conflicting_reader = CustomPrometheusMetricReader()
    conflicting_provider = MeterProvider(metric_readers=[conflicting_reader])
    conflicting_exporter = OtelExporter(conflicting_provider)
    try:
        with pytest.raises(OSError, match="cannot bind"):
            conflicting_exporter.start_http_server(host="127.0.0.1", port=held_server.port)
        conflicting_provider.shutdown()
        with socket.create_connection(("127.0.0.1", held_server.port), timeout=1):
            pass
    finally:
        held_provider.shutdown()


def test_invalid_prometheus_configuration_does_not_bind():
    with pytest.raises(ValueError, match="non-empty"):
        PrometheusHTTPServer(CollectorRegistry(), host="  ", port=9464)
    with pytest.raises(ValueError, match="0 to 65535"):
        PrometheusHTTPServer(CollectorRegistry(), port=70000)
    with pytest.raises(ValueError, match="mutual TLS"):
        PrometheusHTTPServer(
            CollectorRegistry(),
            tls_config=TLSConfig(client_auth_required=True),
        )
    with pytest.raises(ValueError, match="both certfile and keyfile"):
        PrometheusHTTPServer(
            CollectorRegistry(),
            tls_config=TLSConfig(certfile="server.pem"),
        )
    with pytest.raises(ValueError, match="tls_min_version"):
        PrometheusHTTPServer(
            CollectorRegistry(),
            tls_config=TLSConfig(
                tls_min_version=ssl.TLSVersion.TLSv1_3,
                tls_max_version=ssl.TLSVersion.TLSv1_2,
            ),
        )
    with pytest.raises(ValueError, match="does not exist"):
        PrometheusHTTPServer(
            CollectorRegistry(),
            tls_config=TLSConfig(certfile="/no/such.pem", keyfile="/no/such.key"),
        )


def test_concurrent_scrapes_and_shutdown_are_safe():
    reader, server, provider, _exporter = _pair()
    port = server.port
    provider.get_meter("web").create_counter("requests").add(7)
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            scrapes = list(executor.map(lambda _: _scrape(port), range(16)))
        assert all(_value(scrape, "requests_total") == 7 for scrape in scrapes)
    finally:
        errors = []

        def stop():
            try:
                reader.shutdown()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=stop) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        assert errors == []
        assert _closed(port)


def test_callback_failure_and_timeout_do_not_block_snapshot_export():
    started = threading.Event()
    release = threading.Event()

    def hold(options):
        started.set()
        assert release.wait(2)
        return [Observation(1)]

    reader = CustomPrometheusMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    provider.get_meter("hold").create_observable_gauge("hold", callbacks=[hold])
    exporter = OtelExporter(provider)
    server = exporter.start_http_server(host="127.0.0.1", port=0)
    port = server.port
    scrape = threading.Thread(target=lambda: _scrape(port))
    published = threading.Event()

    def publish():
        exporter.export(_Snapshot({"client_type": "python-sdk", "app_id": "orders", "total_nodes": 1}))
        published.set()

    scrape.start()
    try:
        assert started.wait(2)
        threading.Thread(target=publish).start()
        assert published.wait(0.2)
    finally:
        release.set()
        scrape.join(2)
        provider.shutdown()

    failing = {"calls": 0}

    def sometimes(options):
        failing["calls"] += 1
        if failing["calls"] == 1:
            raise RuntimeError("callback failed")
        return [Observation(4)]

    seen = {}

    def observe_budget(options):
        seen["timeout"] = options.timeout_millis
        return [Observation(1)]

    reader, server, provider, _exporter = _pair()
    meter = provider.get_meter("orders")
    meter.create_observable_gauge("broken", callbacks=[sometimes])
    meter.create_observable_gauge("budget", callbacks=[observe_budget])
    port = server.port
    try:
        assert "broken" not in _scrape(port)
        assert _value(_scrape(port), "broken") == 4
        reader.collect(timeout_millis=25)
        assert seen["timeout"] <= 25
    finally:
        provider.shutdown()


def test_prometheus_config_and_second_start():
    reader = CustomPrometheusMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)
    server = exporter.start_http_server(config=PrometheusConfig(host="127.0.0.1", port=0))
    try:
        assert server.registry is reader.registry
        with pytest.raises(RuntimeError, match="already started"):
            exporter.start_http_server(host="127.0.0.1", port=0)
    finally:
        provider.shutdown()


def _bind_child(port: int, queue) -> None:
    from aerospike_client_telemetry.otel.prometheus_reader import (
        CollectorRegistry as ChildRegistry,
    )
    from aerospike_client_telemetry.otel.prometheus_reader import (
        PrometheusHTTPServer as ChildServer,
    )

    server = ChildServer(ChildRegistry(), host="127.0.0.1", port=port)
    try:
        server.start()
    except OSError as exc:
        queue.put(str(exc))
        return
    queue.put("bound")
    server.shutdown()


def test_another_process_cannot_bind_the_same_port():
    _reader, server, provider, _exporter = _pair()
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_bind_child, args=(server.port, queue))
    process.start()
    try:
        message = queue.get(timeout=20)
        process.join(10)
        assert "cannot bind" in message
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        provider.shutdown()


@needs_openssl
def test_exporter_serves_prometheus_over_tls():
    """Server certificate only. A client certificate is not required."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        reader = CustomPrometheusMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        exporter = OtelExporter(provider)
        server = exporter.start_http_server(
            host="127.0.0.1",
            port=0,
            tls_config=TLSConfig(
                certfile=str(root / "server.crt"),
                keyfile=str(root / "server.key"),
                tls_min_version=ssl.TLSVersion.TLSv1_2,
                tls_max_version=ssl.TLSVersion.TLSv1_3,
            ),
        )
        provider.get_meter("orders").create_counter("placed").add(5)
        try:
            with pytest.raises((URLError, ssl.SSLError, ConnectionResetError)):
                _scrape(server.port)
            assert _value(_scrape(server.port, context=tls_client()), "placed_total") == 5
        finally:
            provider.shutdown()


@needs_openssl
def test_exporter_serves_prometheus_over_mutual_tls():
    """The scrape requires a client certificate signed by the configured CA."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mint_certificates(root)
        reader = CustomPrometheusMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        exporter = OtelExporter(provider)
        server = exporter.start_http_server(
            host="127.0.0.1",
            port=0,
            tls_config=TLSConfig(
                certfile=str(root / "server.crt"),
                keyfile=str(root / "server.key"),
                client_cafile=str(root / "ca.crt"),
                client_auth_required=True,
                tls_min_version=ssl.TLSVersion.TLSv1_2,
                tls_max_version=ssl.TLSVersion.TLSv1_3,
            ),
        )
        provider.get_meter("orders").create_counter("placed").add(5)
        try:
            with pytest.raises((URLError, ssl.SSLError)):
                _scrape(server.port, context=tls_client())
            assert _value(
                _scrape(server.port, context=tls_client(root / "client.crt")),
                "placed_total",
            ) == 5
        finally:
            provider.shutdown()


def test_scrape_text_matches_the_pinned_otel_prometheus_reader():
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    resource = Resource.create({"service.name": "orders", "cloud.region": "east"})

    def render(reader_factory):
        registry = CollectorRegistry()
        reader = reader_factory(registry)
        provider = MeterProvider(metric_readers=[reader], resource=resource)
        meter = provider.get_meter("web.app", "1.2.3")
        counter = meter.create_counter(
            "requests.processed", unit="By", description="Processed requests"
        )
        gauge = meter.create_gauge("queue.depth", unit="{item}", description="Depth")
        signed = meter.create_up_down_counter("queue.signed")
        histogram = meter.create_histogram("latency", unit="s", description="Latency")
        try:
            counter.add(2, {"http.method": "GET"})
            counter.add(3, {"http.method": "POST"})
            gauge.set(4, {"lane": "a"})
            signed.add(5)
            signed.add(-1)
            histogram.record(0.2, {"route": "/orders"})
            return generate_latest(registry).decode()
        finally:
            provider.shutdown()

    official = render(lambda registry: PrometheusMetricReader(registry=registry))
    custom = render(lambda registry: CustomPrometheusMetricReader(registry=registry))
    assert custom.splitlines() == official.splitlines()
