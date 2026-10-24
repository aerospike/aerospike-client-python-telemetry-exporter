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

"""The two meter-provider startups."""

import pytest

from aerospike_client_telemetry.otel import OtelExporter, OtelExporterConfig, Protocol, Temporality


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


class _Meter:
    def __init__(self):
        self.gauges = {}
        self.counters = {}

    def create_observable_gauge(self, name, callbacks=None, unit="", description=""):
        self.gauges[name] = list(callbacks or [])

    def create_observable_counter(self, name, callbacks=None, unit="", description=""):
        self.counters[name] = list(callbacks or [])


class _Provider:
    def __init__(self):
        self.meter = _Meter()
        self.shutdowns = 0

    def get_meter(self, name, version=None, schema_url=None):
        self.meter_name = name
        return self.meter

    def shutdown(self):
        self.shutdowns += 1


def _reader(exporter):
    return next(iter(exporter.meter_provider._metric_readers))


def test_exporter_with_no_arguments_builds_a_provider_from_the_environment(monkeypatch):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "orders-service")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", raising=False)
    exporter = OtelExporter()
    try:
        assert exporter.owns_provider
        assert exporter.meter_provider._sdk_config.resource.attributes["service.name"] == "orders-service"
        assert _reader(exporter)._exporter._endpoint == "http://127.0.0.1:4318/v1/metrics"
        exporter.export(_Snapshot({"client_type": "python-sdk", "app_id": "orders-service"}))
    finally:
        exporter.close()
    assert exporter.owns_provider is False
    assert exporter.meter_provider._shutdown is True
    with pytest.raises(RuntimeError, match="closed"):
        exporter.export(_Snapshot({"client_type": "python-sdk", "app_id": "orders-service"}))


def test_endpoint_and_service_name_override_the_environment(monkeypatch):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "from-env")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
    exporter = OtelExporter(
        OtelExporterConfig(service_name="orders", endpoint="http://127.0.0.1:4318")
    )
    try:
        assert exporter.owns_provider
        assert exporter.meter_provider._sdk_config.resource.attributes["service.name"] == "orders"
        assert _reader(exporter)._exporter._endpoint == "http://127.0.0.1:4318/v1/metrics"
    finally:
        exporter.close()


def _observable_counter_temporality(exporter):
    from opentelemetry.sdk.metrics import ObservableCounter

    return _reader(exporter)._exporter._preferred_temporality[ObservableCounter]


def test_unset_temporality_follows_the_environment(monkeypatch):
    from opentelemetry.sdk.metrics.export import AggregationTemporality

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta")
    exporter = OtelExporter(OtelExporterConfig(endpoint="http://127.0.0.1:9"))
    try:
        assert exporter.config.temporality is None
        assert _observable_counter_temporality(exporter) is AggregationTemporality.DELTA
    finally:
        exporter.close()


def test_explicit_temporality_overrides_the_environment(monkeypatch):
    from opentelemetry.sdk.metrics.export import AggregationTemporality

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta")
    exporter = OtelExporter(
        OtelExporterConfig(
            endpoint="http://127.0.0.1:9",
            temporality=Temporality.CUMULATIVE,
        )
    )
    try:
        assert _observable_counter_temporality(exporter) is AggregationTemporality.CUMULATIVE
    finally:
        exporter.close()


def test_http_endpoint_uses_the_http_exporter_and_metrics_path():
    exporter = OtelExporter(
        OtelExporterConfig(endpoint="http://127.0.0.1:4318", protocol=Protocol.HTTP_PROTOBUF)
    )
    try:
        otlp = _reader(exporter)._exporter
        assert otlp.__class__.__module__.endswith("http.metric_exporter")
        assert otlp._endpoint == "http://127.0.0.1:4318/v1/metrics"
    finally:
        exporter.close()


def test_grpc_endpoint_uses_the_grpc_exporter_without_a_metrics_path():
    exporter = OtelExporter(
        OtelExporterConfig(endpoint="http://127.0.0.1:4317", protocol=Protocol.GRPC)
    )
    try:
        otlp = _reader(exporter)._exporter
        assert otlp.__class__.__module__.endswith("grpc.metric_exporter")
        assert otlp._endpoint == "127.0.0.1:4317"
        assert otlp._insecure is True
    finally:
        exporter.close()


def test_proxy_url_is_set_on_the_http_session():
    exporter = OtelExporter(
        OtelExporterConfig(
            endpoint="http://127.0.0.1:9/v1/metrics",
            proxy_url="http://127.0.0.1:8888",
        )
    )
    try:
        session = _reader(exporter)._exporter._client._transport._session
        assert session.proxies["http"] == "http://127.0.0.1:8888"
        assert session.proxies["https"] == "http://127.0.0.1:8888"
    finally:
        exporter.close()


def test_proxy_url_is_rejected_for_grpc():
    with pytest.raises(ValueError, match="proxy_url"):
        OtelExporter(
            OtelExporterConfig(
                endpoint="http://127.0.0.1:9",
                protocol="grpc",
                proxy_url="http://127.0.0.1:8888",
            )
        )


def test_a_meter_provider_is_enough_and_close_leaves_it_running():
    provider = _Provider()
    exporter = OtelExporter(meter_provider=provider)
    document = {
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "cluster_name": "local",
        "app_id": "orders-service",
        "cluster": {"command_count": 4, "nodes": {"active": 1}, "connections": {"opened": 2}},
    }
    exporter.export(_Snapshot(document))
    assert exporter.owns_provider is False
    assert exporter.meter_provider is provider
    assert provider.meter_name == "aerospike.client"
    assert "aerospike_client_command_count" in provider.meter.counters
    observations = list(provider.meter.counters["aerospike_client_command_count"][0](None))
    assert len(observations) == 1
    assert observations[0].value == 4
    assert observations[0].attributes["app_id"] == "orders-service"
    assert observations[0].attributes["cluster"] == "local"
    assert "cluster_name" not in observations[0].attributes
    exporter.close()
    assert provider.shutdowns == 0
    with pytest.raises(RuntimeError, match="closed"):
        exporter.export(_Snapshot(document))


def test_a_provider_plus_an_endpoint_does_not_open_otlp(monkeypatch):
    def fail(self, *args, **kwargs):
        raise AssertionError("this package opened an OTLP exporter")

    http = pytest.importorskip("opentelemetry.exporter.otlp.proto.http.metric_exporter")
    grpc = pytest.importorskip("opentelemetry.exporter.otlp.proto.grpc.metric_exporter")
    monkeypatch.setattr(http.OTLPMetricExporter, "__init__", fail)
    monkeypatch.setattr(grpc.OTLPMetricExporter, "__init__", fail)
    provider = _Provider()
    exporter = OtelExporter(
        OtelExporterConfig(service_name="orders", endpoint="http://127.0.0.1:4318/v1/metrics"),
        meter_provider=provider,
    )
    assert exporter.meter_provider is provider
    assert exporter.owns_provider is False
    assert "aerospike_client_total_nodes" in provider.meter.gauges
    exporter.close()
    assert provider.shutdowns == 0
