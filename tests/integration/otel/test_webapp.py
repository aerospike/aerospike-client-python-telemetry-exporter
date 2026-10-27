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

"""How a customer webapp calls the exporter.

``OrdersWebapp`` is the application. It builds one exporter, connects with
``CUSTOM`` mode, registers that exporter, serves a request, then shuts down
in the order the application owns: disable metrics, close the cluster, close
the exporter once. The test waits on the client's real scheduler.
"""

from __future__ import annotations

from datetime import timedelta
import os
import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsExporterType, MetricsPolicy
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.common import client_app_ip, series_attributes
from aerospike_client_telemetry.otel import OtelExporter
from tests.fake_provider import RecordingProvider
from tests.integration.support import CALLBACK_TIMEOUT_SECONDS, namespace, seed_host

pytestmark = pytest.mark.integration

# Shortened so this test can observe one real interval. A service would use
# its own interval, often 30 seconds or more.
_EXPORT_INTERVAL = timedelta(seconds=1)
_OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
_OTLP_GRPC_ENDPOINT = "OTEL_EXPORTER_OTLP_GRPC_ENDPOINT"
_OTLP_HEADERS = "OTEL_EXPORTER_OTLP_HEADERS"


def _webapp_labels() -> dict[str, str]:
    return {"environment": "prod", "region": "us-west", "client_app_ip": client_app_ip()}


class OrdersWebapp:
    """Sync service that stores orders and exports client telemetry."""

    def __init__(self, namespace_name: str) -> None:
        self._namespace = namespace_name
        self.cluster = None
        self.exporter = OtelExporter(RecordingProvider())

    def start(self, host: str, port: int) -> None:
        self.cluster = (
            ClusterDefinition(host, port)
            .app_id("orders-service")
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
        self.cluster.add_exporter(self.exporter)
        self.cluster.enable_metrics(
            MetricsPolicy(
                # Extended metrics are two tiers: operational (latency, bytes,
                # result codes, command count) and usage (which SDK features
                # the application called).
                operational_enabled=True,
                usage_enabled=True,
                labels=_webapp_labels(),
            )
        )

    def place_order(self, order_id: int, sku: str) -> dict:
        session = self.cluster.create_session()
        key = DataSet.of(self._namespace, "orders").id(order_id)
        session.upsert(key).put({"sku": sku, "qty": 1}).execute()
        return session.get(key).bins

    def shutdown(self) -> None:
        if self.cluster is not None:
            self.cluster.disable_metrics()
            self.cluster.close()
            self.cluster = None
        self.exporter.close()


def _connect(exporter: OtelExporter, host: str, port: int):
    """One Aerospike client in the webapp, registered on the shared exporter."""
    cluster = (
        ClusterDefinition(host, port)
        .app_id("orders-service")
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
    cluster.add_exporter(exporter)
    cluster.enable_metrics(
        MetricsPolicy(
            operational_enabled=True,
            usage_enabled=True,
            labels=_webapp_labels(),
        )
    )
    return cluster


def _place(cluster, namespace_name: str, order_id: int) -> None:
    session = cluster.create_session()
    key = DataSet.of(namespace_name, "orders").id(order_id)
    session.upsert(key).put({"sku": f"sku-{order_id}", "qty": 1}).execute()


def _document_for(exporter: OtelExporter, cluster_name: str, minimum: int = 1) -> dict:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        for key in exporter.registry.client_keys():
            document = exporter.registry.current(key)
            if document and document.get("cluster_name") == cluster_name:
                count = document["command_count"]
                if count >= minimum:
                    return document
        time.sleep(0.05)
    raise AssertionError(f"no snapshot for cluster {cluster_name}")


def _snapshot_for(app: OrdersWebapp, app_id: str) -> dict:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        for key in app.exporter.registry.client_keys():
            document = app.exporter.registry.current(key)
            if document and document.get("app_id") == app_id:
                return document
        time.sleep(0.05)
    raise AssertionError("periodic scheduler did not call export()")


def test_orders_webapp_exports_client_telemetry():
    host, port = seed_host()
    app = OrdersWebapp(namespace())
    try:
        app.start(host, port)
    except Exception as exc:
        app.shutdown()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        stored = app.place_order(42, "sku-1")
        assert stored["sku"] == "sku-1"

        document = _snapshot_for(app, "orders-service")
        assert document["client_type"] == "python-sdk"
        assert document["app_id"] == "orders-service"
        assert document["labels"] == _webapp_labels()
        assert series_attributes(document)["client_app_ip"] == client_app_ip()
        assert document["operational_metrics_enabled"] is True
        assert document["usage_metrics_enabled"] is True
        assert document["usage"]["blocking"] >= 1
        assert document["usage"]["shape_point"] >= 1
        assert document["command_count"] >= 1
        assert document["total_nodes"] >= 1
        assert document["nodes"]
        assert app.exporter.meter_provider.meter_name == "aerospike.client"
    finally:
        app.shutdown()


def test_one_exporter_keeps_two_clusters_independent(aerospike_server_two):
    host, port = seed_host()
    other_host, _, other_port = aerospike_server_two.rpartition(":")
    namespace_name = namespace()
    exporter = OtelExporter(RecordingProvider())
    clusters = []
    try:
        clusters.append(_connect(exporter, host, port))
        clusters.append(_connect(exporter, other_host, int(other_port)))
        _place(clusters[0], namespace_name, 1)
        _place(clusters[1], namespace_name, 2)
        _place(clusters[1], namespace_name, 3)
        first = _document_for(exporter, "telemetry-it")
        second = _document_for(exporter, "telemetry-it-two", minimum=2)
        assert len(exporter.registry.client_keys()) == 2
        assert clusters[0].exporters == (exporter,)
        assert clusters[1].exporters == (exporter,)
        assert first["app_id"] == second["app_id"] == "orders-service"
        assert first["cluster_name"] == "telemetry-it"
        assert second["cluster_name"] == "telemetry-it-two"
        assert first["command_count"] >= 1
        held = second["command_count"]
        assert held >= 2
        _place(clusters[0], namespace_name, 4)
        moved = _document_for(exporter, "telemetry-it")
        deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
        while moved["command_count"] <= first["command_count"]:
            if time.monotonic() > deadline:
                break
            time.sleep(0.05)
            moved = _document_for(exporter, "telemetry-it")
        assert moved["command_count"] > first["command_count"]
        still = _document_for(exporter, "telemetry-it-two")
        assert still["command_count"] == held
    finally:
        for cluster in clusters:
            cluster.disable_metrics()
            cluster.close()
        exporter.close()


def _cloud_otlp(grpc: bool) -> tuple[str, dict[str, str]]:
    """Endpoint and auth headers for one OTLP protocol, or skip that case."""
    endpoint_var = _OTLP_GRPC_ENDPOINT if grpc else _OTLP_ENDPOINT
    protocol = "gRPC" if grpc else "HTTP"
    endpoint = os.environ.get(endpoint_var, "").strip()
    raw_headers = os.environ.get(_OTLP_HEADERS, "").strip()
    if not endpoint or not raw_headers:
        pytest.skip(
            f"set {endpoint_var} and {_OTLP_HEADERS} to send this webapp over {protocol}"
        )
    headers: dict[str, str] = {}
    for item in raw_headers.split(","):
        item = item.strip()
        if not item:
            continue
        name, separator, value = item.partition("=")
        if not separator or not name.strip() or not value.strip():
            pytest.fail(
                f"{_OTLP_HEADERS} must be comma-separated key=value pairs, "
                "for example api-key=<license>"
            )
        headers[name.strip()] = value.strip()
    if not headers:
        pytest.skip(
            f"set {endpoint_var} and {_OTLP_HEADERS} to send this webapp over {protocol}"
        )
    return endpoint, headers


def _cloud_provider(endpoint: str, headers: dict[str, str], *, grpc: bool):
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
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            },
        ),
        export_interval_millis=1000,
    )
    return MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"service.name": "python-orders-service"}),
    )


class _CloudApp:
    """The slice of ``OrdersWebapp`` that ``_snapshot_for`` reads."""

    def __init__(self, exporter: OtelExporter) -> None:
        self.exporter = exporter


@pytest.mark.parametrize("grpc", [False, True], ids=["http", "grpc"])
def test_webapp_sends_to_an_otlp_cloud_when_configured(grpc: bool):
    """Push one order snapshot to a real OTLP cloud over HTTP and over gRPC.

    HTTP reads ``OTEL_EXPORTER_OTLP_ENDPOINT``, for example
    ``https://otlp.nr-data.net/v1/metrics``. gRPC reads
    ``OTEL_EXPORTER_OTLP_GRPC_ENDPOINT``, for example
    ``https://otlp.nr-data.net:4317``. Both read
    ``OTEL_EXPORTER_OTLP_HEADERS``, for example ``api-key=<license>``.
    CI passes the same names from Actions secrets. A missing endpoint or
    header skips only that protocol. New Relic sees ``service.name``
    ``python-orders-service``.
    """
    endpoint, headers = _cloud_otlp(grpc)
    app_id = "orders-grpc" if grpc else "orders-http"
    host, port = seed_host()
    provider = _cloud_provider(endpoint, headers, grpc=grpc)
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
        exporter.close()
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
        _snapshot_for(_CloudApp(exporter), app_id)
        assert provider.force_flush(30000) is True
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        exporter.close()
        provider.shutdown()
