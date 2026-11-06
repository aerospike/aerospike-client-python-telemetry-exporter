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

"""One OTLP case per cloud endpoint.

Each provider reads its own endpoint and header variables. A missing URL or
required header skips only that case. CI passes the same names from Actions
secrets.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import os
import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsExporterType, MetricsPolicy
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.common import client_app_ip
from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.support import CALLBACK_TIMEOUT_SECONDS, namespace, seed_host

pytestmark = pytest.mark.integration

_EXPORT_INTERVAL = timedelta(seconds=1)


@dataclass(frozen=True)
class _CloudTarget:
    """One OTLP backend. A missing URL or required header skips that case."""

    name: str
    http_var: str
    grpc_var: str
    headers_var: str
    http_example: str
    grpc_example: str
    header_example: str
    headers_required: bool = True
    delta_gauges: bool = False


# New Relic keeps the original variable names so existing CI secrets still apply.
# gRPC is omitted for backends whose public intake is HTTP only.
_CLOUDS = (
    _CloudTarget(
        "newrelic",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_GRPC_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "https://otlp.nr-data.net/v1/metrics",
        "otlp.nr-data.net:4317",
        "api-key=<license>",
    ),
    _CloudTarget(
        "datadog",
        "OTEL_EXPORTER_OTLP_ENDPOINT_DATADOG",
        "",
        "OTEL_EXPORTER_OTLP_HEADERS_DATADOG",
        "https://otlp.datadoghq.com/v1/metrics",
        "",
        "dd-api-key=<key>",
        delta_gauges=True,
    ),
    _CloudTarget(
        "dynatrace",
        "OTEL_EXPORTER_OTLP_ENDPOINT_DYNATRACE",
        "OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_DYNATRACE",
        "OTEL_EXPORTER_OTLP_HEADERS_DYNATRACE",
        "https://{env-id}.live.dynatrace.com/api/v2/otlp/v1/metrics",
        "{env-id}.live.dynatrace.com:443",
        "Authorization=Bearer <token>",
    ),
    _CloudTarget(
        "cloudwatch",
        "OTEL_EXPORTER_OTLP_ENDPOINT_CLOUDWATCH",
        "",
        "OTEL_EXPORTER_OTLP_HEADERS_CLOUDWATCH",
        "https://monitoring.{region}.amazonaws.com/v1/metrics",
        "",
        "Authorization=Bearer <token>",
    ),
    _CloudTarget(
        "google-cloud",
        "OTEL_EXPORTER_OTLP_ENDPOINT_GOOGLE_CLOUD",
        "OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_GOOGLE_CLOUD",
        "OTEL_EXPORTER_OTLP_HEADERS_GOOGLE_CLOUD",
        "https://telemetry.googleapis.com/v1/metrics",
        "telemetry.googleapis.com:443",
        "Authorization=Bearer <token>",
    ),
    _CloudTarget(
        "grafana",
        "OTEL_EXPORTER_OTLP_ENDPOINT_GRAFANA",
        "OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_GRAFANA",
        "OTEL_EXPORTER_OTLP_HEADERS_GRAFANA",
        "https://otlp-gateway-{region}.grafana.net/otlp/v1/metrics",
        "otlp-gateway-{region}.grafana.net:443",
        "Authorization=Basic <token>",
    ),
    _CloudTarget(
        "prometheus",
        "OTEL_EXPORTER_OTLP_ENDPOINT_PROMETHEUS",
        "",
        "OTEL_EXPORTER_OTLP_HEADERS_PROMETHEUS",
        "http://127.0.0.1:9090/api/v1/otlp/v1/metrics",
        "",
        "",
        headers_required=False,
    ),
    _CloudTarget(
        "sumologic",
        "OTEL_EXPORTER_OTLP_ENDPOINT_SUMOLOGIC",
        "",
        "OTEL_EXPORTER_OTLP_HEADERS_SUMOLOGIC",
        "https://{collection-host}/receiver/v1/otlp/{token}/v1/metrics",
        "",
        "x-sumo-token=<token>",
        headers_required=False,
    ),
    _CloudTarget(
        "openobserve",
        "OTEL_EXPORTER_OTLP_ENDPOINT_OPENOBSERVE",
        "OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_OPENOBSERVE",
        "OTEL_EXPORTER_OTLP_HEADERS_OPENOBSERVE",
        "https://{host}/api/{org}/v1/metrics",
        "{host}:5081",
        "Authorization=Basic <token>",
    ),
)


def _webapp_labels() -> dict[str, str]:
    return {"environment": "prod", "region": "us-west", "client_app_ip": client_app_ip()}


def _cloud_cases() -> list:
    cases = []
    for cloud in _CLOUDS:
        cases.append(pytest.param(cloud, False, id=f"{cloud.name}-http"))
        if cloud.grpc_var:
            cases.append(pytest.param(cloud, True, id=f"{cloud.name}-grpc"))
    return cases


def _header_map(cloud: _CloudTarget) -> dict[str, str]:
    raw_headers = os.environ.get(cloud.headers_var, "").strip()
    headers: dict[str, str] = {}
    for item in raw_headers.split(","):
        item = item.strip()
        if not item:
            continue
        name, separator, value = item.partition("=")
        if not separator or not name.strip() or not value.strip():
            pytest.fail(
                f"{cloud.headers_var} must be comma-separated key=value pairs, "
                f"for example {cloud.header_example}"
            )
        headers[name.strip()] = value.strip()
    return headers


def _cloud_otlp(cloud: _CloudTarget, grpc: bool) -> tuple[str, dict[str, str]]:
    """Endpoint and auth headers for one backend, or skip that case."""
    endpoint_var = cloud.grpc_var if grpc else cloud.http_var
    protocol = "gRPC" if grpc else "HTTP"
    example = cloud.grpc_example if grpc else cloud.http_example
    endpoint = os.environ.get(endpoint_var, "").strip()
    if not endpoint:
        pytest.skip(
            f"set {endpoint_var} to send {cloud.name} over {protocol}, for example {example}"
        )
    headers = _header_map(cloud)
    if cloud.headers_required and not headers:
        pytest.skip(f"set {cloud.headers_var} for {cloud.name}, for example {cloud.header_example}")
    return endpoint, headers


def _cloud_provider(
    endpoint: str,
    headers: dict[str, str],
    *,
    grpc: bool,
    delta_gauges: bool = False,
):
    """The webapp pipeline for one protocol, with the cloud auth header."""
    if grpc:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
    else:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
    from opentelemetry.sdk.metrics import (
        Counter,
        Histogram,
        MeterProvider,
        ObservableCounter,
        ObservableGauge,
        ObservableUpDownCounter,
        UpDownCounter,
    )
    from opentelemetry.sdk.metrics.export import (
        AggregationTemporality,
        PeriodicExportingMetricReader,
    )
    from opentelemetry.sdk.resources import Resource

    delta = AggregationTemporality.DELTA
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(
            endpoint=endpoint,
            headers=headers,
            preferred_temporality={
                Counter: delta,
                UpDownCounter: delta,
                Histogram: delta,
                ObservableCounter: delta,
                ObservableUpDownCounter: delta,
                ObservableGauge: delta if delta_gauges else AggregationTemporality.CUMULATIVE,
            },
        ),
        export_interval_millis=1000,
    )
    return MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"service.name": "python-orders-service"}),
    )


def _snapshot_for(exporter: OtelExporter, app_id: str) -> dict:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        for key in exporter.registry.client_keys():
            document = exporter.registry.current(key)
            if document and document.get("app_id") == app_id:
                return document
        time.sleep(0.05)
    raise AssertionError("periodic scheduler did not call export()")


@pytest.mark.parametrize(("cloud", "grpc"), _cloud_cases())
def test_webapp_sends_to_an_otlp_cloud_when_configured(cloud: _CloudTarget, grpc: bool):
    """Push one order snapshot to a configured OTLP backend.

    Each provider reads its own endpoint and header variables. A missing URL
    or required header skips only that case. CI passes the same names from
    Actions secrets. The HTTP URL is the metrics path the exporter POSTs to.
    Datadog's intake accepts delta points, so its gauges are delta too.
    CloudWatch's usual auth is SigV4, which this case does not sign. A bearer
    token in the header variable is what the case can send. The resource
    ``service.name`` is ``python-orders-service``.
    """
    endpoint, headers = _cloud_otlp(cloud, grpc)
    protocol = "grpc" if grpc else "http"
    app_id = f"orders-{cloud.name}-{protocol}"
    host, port = seed_host()
    provider = _cloud_provider(
        endpoint,
        headers,
        grpc=grpc,
        delta_gauges=cloud.delta_gauges,
    )
    exporter = OtelExporter(provider)
    cluster = None
    try:
        cluster = (
            ClusterDefinition(host, port)
            .app_id(app_id)
            .with_system_settings(
                SystemSettings(
                    metrics=MetricsSettings(
                        exporter=MetricsExporterType.CUSTOM,
                        export_interval=_EXPORT_INTERVAL,
                    )
                )
            )
            .connect()
        )
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        cluster.enable_metrics(
            MetricsPolicy(
                operational_enabled=True,
                usage_enabled=True,
                labels=_webapp_labels(),
            )
        )
        session = cluster.create_session()
        key = DataSet.of(namespace(), "orders").id(42)
        session.upsert(key).put({"sku": "sku-1", "qty": 1}).execute()
        _snapshot_for(exporter, app_id)
        assert provider.force_flush(30000) is True
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        provider.shutdown()
