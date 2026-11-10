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

import inspect
import logging
import multiprocessing
import signal
import socket
import ssl
import subprocess
import tempfile
import threading
import time
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


@needs_openssl
def test_invalid_or_mismatched_tls_material_does_not_bind():
    """Bad certificate, key, or CA contents fail before the port is bound."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        mint_certificates(root)
        _openssl_req(root, "other.key")
        (root / "garbage.crt").write_text("not a certificate\n")
        (root / "garbage.key").write_text("not a key\n")
        (root / "empty-cas").mkdir()
        mixed = root / "mixed.crt"
        mixed.write_text((root / "ca.crt").read_text() + (root / "server.crt").read_text())
        server_crt = str(root / "server.crt")
        server_key = str(root / "server.key")
        PrometheusHTTPServer(
            CollectorRegistry(),
            host="127.0.0.1",
            port=0,
            tls_config=TLSConfig(
                certfile=server_crt,
                keyfile=server_key,
                client_cafile=str(root / "ca.crt"),
                client_auth_required=True,
            ),
        )
        rejected = (
            (
                TLSConfig(certfile=str(root / "garbage.crt"), keyfile=server_key),
                "cannot load server certificate or key",
            ),
            (
                TLSConfig(certfile=server_crt, keyfile=str(root / "garbage.key")),
                "cannot load server certificate or key",
            ),
            (
                TLSConfig(certfile=server_crt, keyfile=str(root / "other.key")),
                "cannot load server certificate or key",
            ),
            (
                TLSConfig(
                    certfile=server_crt,
                    keyfile=server_key,
                    client_cafile=str(root / "garbage.crt"),
                    client_auth_required=True,
                ),
                "cannot load client CA",
            ),
            (
                TLSConfig(
                    certfile=server_crt,
                    keyfile=server_key,
                    client_cafile=server_crt,
                    client_auth_required=True,
                ),
                "does not contain a CA certificate",
            ),
            (
                TLSConfig(
                    certfile=server_crt,
                    keyfile=server_key,
                    client_cafile=str(mixed),
                    client_auth_required=True,
                ),
                "not a CA",
            ),
            (
                TLSConfig(
                    certfile=server_crt,
                    keyfile=server_key,
                    client_capath=str(root / "empty-cas"),
                    client_auth_required=True,
                ),
                "does not contain a CA certificate",
            ),
        )
        for tls_config, message in rejected:
            with pytest.raises(ValueError, match=message):
                PrometheusHTTPServer(
                    CollectorRegistry(),
                    host="127.0.0.1",
                    port=0,
                    tls_config=tls_config,
                )


def _openssl_req(directory: Path, key_name: str) -> None:
    subprocess.run(
        [
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key_name,
            "-out",
            "other.csr",
            "-subj",
            "/CN=other",
        ],
        cwd=directory,
        check=True,
        capture_output=True,
        text=True,
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
        exporter.export(
            _Snapshot(
                {
                    "client_type": "python-sdk",
                    "app_id": "orders",
                    "cluster": {"nodes": {"active": 1}},
                }
            )
        )
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


def test_a_slow_callback_runs_to_completion():
    """A callback longer than the collect hint is not interrupted."""
    seen = {}

    def slow(options):
        seen["timeout"] = options.timeout_millis
        time.sleep(0.2)
        return [Observation(1)]

    reader = CustomPrometheusMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    provider.get_meter("slow").create_observable_gauge("slow", callbacks=[slow])
    try:
        began = time.monotonic()
        reader.collect(timeout_millis=50)
        assert time.monotonic() - began >= 0.2
        assert seen["timeout"] <= 50
        assert _value(generate_latest(reader.registry).decode(), "slow") == 1
    finally:
        provider.shutdown()


def test_shutdown_returns_while_a_scrape_callback_is_blocked():
    """A callback blocked forever does not keep shutdown waiting."""
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocked(options):
        started.set()
        release.wait()
        finished.set()
        return [Observation(1)]

    reader = CustomPrometheusMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    provider.get_meter("hold").create_observable_gauge("hold", callbacks=[blocked])
    exporter = OtelExporter(provider)
    server = exporter.start_http_server(host="127.0.0.1", port=0)
    port = server.port

    def scrape() -> None:
        try:
            _scrape(port)
        except Exception:
            pass

    worker = threading.Thread(target=scrape, daemon=True)
    worker.start()
    assert started.wait(2)
    try:
        began = time.monotonic()
        reader.shutdown(timeout_millis=1_000)
        assert time.monotonic() - began < 2
        assert not finished.is_set()
        assert _closed(port)
    finally:
        release.set()
        worker.join(2)
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


def test_gauge_and_counter_text_matches_the_official_prometheus_reader():
    """Sync and observable gauges and counters scrape to the same text."""
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    resource = Resource.create({"service.name": "orders"})

    def render(reader_factory):
        registry = CollectorRegistry()
        reader = reader_factory(registry)
        provider = MeterProvider(metric_readers=[reader], resource=resource)
        meter = provider.get_meter("orders", "1.0.0")
        placed = meter.create_counter(
            "orders.placed", unit="{order}", description="Orders placed"
        )
        backlog = meter.create_gauge(
            "orders.backlog", unit="{order}", description="Orders waiting"
        )
        meter.create_observable_counter(
            "orders.observed",
            callbacks=[lambda options: [Observation(5)]],
            description="Observed orders",
        )
        meter.create_observable_gauge(
            "orders.depth",
            callbacks=[lambda options: [Observation(3)]],
            description="Observed depth",
        )
        try:
            placed.add(2, {"lane": "a"})
            placed.add(1, {"lane": "b"})
            backlog.set(4)
            return generate_latest(registry).decode()
        finally:
            provider.shutdown()

    official = render(lambda registry: PrometheusMetricReader(registry=registry))
    custom = render(lambda registry: CustomPrometheusMetricReader(registry=registry))
    assert custom.splitlines() == official.splitlines()
    assert "# TYPE orders_placed_total counter" in custom
    assert "# TYPE orders_backlog gauge" in custom
    assert "# TYPE orders_observed_total counter" in custom
    assert "# TYPE orders_depth gauge" in custom
    assert _value(custom, "orders_placed_total") == 2
    assert _value(custom, "orders_backlog") == 4
    assert _value(custom, "orders_observed_total") == 5
    assert _value(custom, "orders_depth") == 3


def test_filter_and_registry_are_keyword_only_like_the_official_reader():
    """The star in ``__init__`` matches ``PrometheusMetricReader``.

    ``disable_target_info``, ``prefix``, and ``scope_info_enabled`` may be
    positional. ``resource_attribute_filter`` and ``registry`` are keyword-only.
    """
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    custom = inspect.signature(CustomPrometheusMetricReader.__init__)
    official = inspect.signature(PrometheusMetricReader.__init__)
    names = (
        "disable_target_info",
        "prefix",
        "scope_info_enabled",
        "resource_attribute_filter",
        "registry",
    )
    for name in names:
        assert custom.parameters[name].kind is official.parameters[name].kind
    assert custom.parameters["disable_target_info"].default is False
    assert custom.parameters["prefix"].default == ""
    assert custom.parameters["scope_info_enabled"].default is True
    assert custom.parameters["resource_attribute_filter"].default is None
    assert custom.parameters["resource_attribute_filter"].kind is inspect.Parameter.KEYWORD_ONLY
    assert custom.parameters["registry"].kind is inspect.Parameter.KEYWORD_ONLY
    with pytest.raises(TypeError):
        CustomPrometheusMetricReader(False, "", True, None)


def _record_depth(meter) -> None:
    meter.create_counter(
        "requests.processed", unit="By", description="Processed requests"
    ).add(2)
    meter.create_gauge("queue.depth", description="Current queue depth").set(4)


def _paired_scrape(record, **settings) -> str:
    """Scrape the same instruments with both readers and require equal text."""
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    resource = Resource.create({"service.name": "orders", "cloud.region": "east"})

    def render(reader_factory):
        registry = CollectorRegistry()
        reader = reader_factory(registry)
        provider = MeterProvider(metric_readers=[reader], resource=resource)
        try:
            record(provider.get_meter("web.app", "1.2.3"))
            return generate_latest(registry).decode()
        finally:
            provider.shutdown()

    official = render(lambda registry: PrometheusMetricReader(registry=registry, **settings))
    custom = render(lambda registry: CustomPrometheusMetricReader(registry=registry, **settings))
    assert custom.splitlines() == official.splitlines()
    return custom


def _series_line(text: str, name: str) -> str:
    for line in text.splitlines():
        if line.startswith(name + "{") or line.startswith(name + " "):
            return line
    raise AssertionError(f"{name} is absent from the Prometheus scrape")


def test_disable_target_info_matches_the_official_reader():
    shown = _paired_scrape(_record_depth, disable_target_info=False)
    assert "# TYPE target_info gauge" in shown
    info = _series_line(shown, "target_info")
    assert 'service_name="orders"' in info
    assert 'cloud_region="east"' in info
    hidden = _paired_scrape(_record_depth, disable_target_info=True)
    assert "target_info" not in hidden
    assert _value(hidden, "queue_depth") == 4
    assert _value(hidden, "requests_processed_bytes_total") == 2


def test_prefix_matches_the_official_reader():
    plain = _paired_scrape(_record_depth, prefix="")
    assert "# TYPE queue_depth gauge" in plain
    assert "app_queue_depth" not in plain
    prefixed = _paired_scrape(_record_depth, prefix="app")
    assert "# TYPE app_queue_depth gauge" in prefixed
    assert "# TYPE app_requests_processed_bytes_total counter" in prefixed
    assert "# TYPE target_info gauge" in prefixed
    assert not any(line.startswith("queue_depth") for line in prefixed.splitlines())


def test_scope_info_matches_the_official_reader():
    enabled = _paired_scrape(_record_depth, scope_info_enabled=True)
    depth = _series_line(enabled, "queue_depth")
    assert 'otel_scope_name="web.app"' in depth
    assert 'otel_scope_version="1.2.3"' in depth
    assert 'otel_scope_schema_url=""' in depth
    disabled = _paired_scrape(_record_depth, scope_info_enabled=False)
    depth = _series_line(disabled, "queue_depth")
    assert "otel_scope_" not in depth
    assert _value(disabled, "queue_depth") == 4


def test_resource_attribute_filter_matches_the_official_reader():
    unfiltered = _paired_scrape(_record_depth, resource_attribute_filter=None)
    depth = _series_line(unfiltered, "queue_depth")
    assert "service_name" not in depth
    assert "cloud_region" not in depth
    info = _series_line(unfiltered, "target_info")
    assert 'service_name="orders"' in info
    assert 'cloud_region="east"' in info

    filtered = _paired_scrape(
        _record_depth,
        resource_attribute_filter=lambda name: name == "service.name",
    )
    depth = _series_line(filtered, "queue_depth")
    assert 'service_name="orders"' in depth
    assert "cloud_region" not in depth
    info = _series_line(filtered, "target_info")
    assert 'service_name="orders"' in info
    assert 'cloud_region="east"' in info


def test_registry_matches_the_official_reader():
    """An explicit registry receives the scrape and the process registry does not."""
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from prometheus_client import REGISTRY

    resource = Resource.create({"service.name": "orders"})

    def render(reader, registry) -> str:
        provider = MeterProvider(metric_readers=[reader], resource=resource)
        try:
            provider.get_meter("web.app", "1.2.3").create_gauge("queue.depth").set(4)
            return generate_latest(registry).decode()
        finally:
            provider.shutdown()

    custom_registry = CollectorRegistry()
    official_registry = CollectorRegistry()
    custom_text = render(
        CustomPrometheusMetricReader(registry=custom_registry), custom_registry
    )
    official_text = render(
        PrometheusMetricReader(registry=official_registry), official_registry
    )
    assert custom_text.splitlines() == official_text.splitlines()
    assert "queue_depth" not in generate_latest(REGISTRY).decode()

    private = CustomPrometheusMetricReader()
    private_text = render(private, private.registry)
    fresh = CollectorRegistry()
    official_private = render(PrometheusMetricReader(registry=fresh), fresh)
    assert private_text.splitlines() == official_private.splitlines()
    assert private.registry is not REGISTRY


def test_preferred_temporality_matches_the_official_reader():
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    custom = CustomPrometheusMetricReader(registry=CollectorRegistry())
    official = PrometheusMetricReader(registry=CollectorRegistry())
    try:
        assert custom._instrument_class_temporality == official._instrument_class_temporality
        assert set(custom._instrument_class_temporality.values()) == {
            AggregationTemporality.CUMULATIVE
        }
    finally:
        custom.shutdown()
        official.shutdown()

    def record(meter) -> None:
        counter = meter.create_counter("requests.processed", description="Processed requests")
        counter.add(2)
        counter.add(3)

    text = _paired_scrape(record)
    assert _value(text, "requests_processed_total") == 5
