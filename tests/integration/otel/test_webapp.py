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
import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsExporterType, MetricsPolicy
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.common import client_app_ip, series_attributes
from aerospike_client_telemetry.otel import OtelExporter, OtelExporterConfig, Protocol
from tests.integration.support import CALLBACK_TIMEOUT_SECONDS, namespace, seed_host

pytestmark = pytest.mark.integration

# Shortened so this test can observe one real interval. A service would use
# its own interval, often 30 seconds or more.
_EXPORT_INTERVAL = timedelta(seconds=1)


def _webapp_labels() -> dict[str, str]:
    return {"environment": "prod", "region": "us-west", "client_app_ip": client_app_ip()}


class OrdersWebapp:
    """Sync service that stores orders and exports client telemetry."""

    def __init__(self, namespace_name: str) -> None:
        self._namespace = namespace_name
        self.cluster = None
        self.exporter = OtelExporter(
            OtelExporterConfig(
                service_name="orders-service",
                endpoint="http://127.0.0.1:4318/v1/metrics",
                protocol=Protocol.HTTP_PROTOBUF,
                headers={"authorization": "Bearer orders-token"},
            )
        )

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
                count = document["cluster"]["command_count"]
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
        assert document["usage"]["feature.api.blocking"] >= 1
        assert document["usage"]["feature.shape.point"] >= 1
        assert document["cluster"]["command_count"] >= 1
        assert document["cluster"]["nodes"]["active"] >= 1
        assert document["nodes"]
        assert app.exporter.config.service_name == "orders-service"
        assert app.exporter.config.protocol is Protocol.HTTP_PROTOBUF
        assert app.exporter.config.headers == {"authorization": "Bearer orders-token"}
    finally:
        app.shutdown()


def test_one_exporter_keeps_two_clusters_independent(aerospike_server_two):
    host, port = seed_host()
    other_host, _, other_port = aerospike_server_two.rpartition(":")
    namespace_name = namespace()
    exporter = OtelExporter(
        OtelExporterConfig(
            service_name="orders-service",
            endpoint="http://127.0.0.1:4318/v1/metrics",
            protocol=Protocol.HTTP_PROTOBUF,
            headers={"authorization": "Bearer orders-token"},
        )
    )
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
        assert first["cluster"]["command_count"] >= 1
        held = second["cluster"]["command_count"]
        assert held >= 2
        _place(clusters[0], namespace_name, 4)
        moved = _document_for(exporter, "telemetry-it")
        deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
        while moved["cluster"]["command_count"] <= first["cluster"]["command_count"]:
            if time.monotonic() > deadline:
                break
            time.sleep(0.05)
            moved = _document_for(exporter, "telemetry-it")
        assert moved["cluster"]["command_count"] > first["cluster"]["command_count"]
        still = _document_for(exporter, "telemetry-it-two")
        assert still["cluster"]["command_count"] == held
    finally:
        for cluster in clusters:
            cluster.disable_metrics()
            cluster.close()
        exporter.close()
