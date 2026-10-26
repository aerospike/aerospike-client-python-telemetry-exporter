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

"""Several exporters on one cluster, and two clusters with their own exporters.

The client accepts both registrations. Two objects keep two registries. The
same object registered twice is called twice in one cycle and still stores
one document. Two clusters that each register a different exporter do not
share a registry.
"""

from __future__ import annotations

import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.fake_provider import RecordingProvider
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    custom_metrics_settings,
    namespace,
    seed_host,
)

pytestmark = pytest.mark.integration


class _CountingExporter(OtelExporter):
    def __init__(self) -> None:
        super().__init__(RecordingProvider())
        self.calls = 0

    def export(self, snapshot) -> None:
        self.calls += 1
        super().export(snapshot)


def _connect(host: str, port: int, app_id: str = "orders-service"):
    return (
        ClusterDefinition(host, port)
        .app_id(app_id)
        .with_system_settings(custom_metrics_settings())
        .connect()
    )


def _write(cluster) -> None:
    session = cluster.create_session()
    session.upsert(DataSet.of(namespace(), "multi_exporter").id(1)).put({"n": 1}).execute()


def _wait(predicate, message: str) -> None:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(message)


def _shutdown(cluster, *exporters) -> None:
    if cluster is not None:
        cluster.disable_metrics()
        cluster.close()
    for exporter in exporters:
        exporter.close()


def test_two_otel_exporters_each_receive_the_snapshot():
    host, port = seed_host()
    orders = _CountingExporter()
    billing = _CountingExporter()
    cluster = None
    try:
        cluster = _connect(host, port)
    except Exception as exc:
        orders.close()
        billing.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(orders)
        cluster.add_exporter(billing)
        cluster.enable_metrics(MetricsPolicy(operational_enabled=True))
        _write(cluster)
        _wait(
            lambda: orders.calls >= 1 and billing.calls >= 1,
            "both exporters were not called",
        )
        assert orders.registry is not billing.registry
        assert len(orders.registry.client_keys()) == 1
        assert len(billing.registry.client_keys()) == 1
        orders_document = orders.registry.current(orders.registry.client_keys()[0])
        billing_document = billing.registry.current(billing.registry.client_keys()[0])
        assert orders_document["app_id"] == "orders-service"
        assert billing_document["app_id"] == "orders-service"
        assert orders_document["client_type"] == "python-sdk"
    finally:
        _shutdown(cluster, orders, billing)


def _metrics(exporter: _CountingExporter) -> dict:
    return dict(exporter.registry.current(exporter.registry.client_keys()[0]))


def test_three_exporters_receive_the_same_metrics():
    host, port = seed_host()
    exporters = [
        _CountingExporter() for _name in ("orders", "billing", "shipping")
    ]
    cluster = None
    try:
        cluster = _connect(host, port)
    except Exception as exc:
        for exporter in exporters:
            exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        for exporter in exporters:
            cluster.add_exporter(exporter)
        cluster.enable_metrics(MetricsPolicy(operational_enabled=True, usage_enabled=True))
        _write(cluster)
        _wait(
            lambda: all(exporter.calls >= 1 for exporter in exporters),
            "not every exporter was called",
        )
        metrics = [_metrics(exporter) for exporter in exporters]
        assert metrics[0] == metrics[1] == metrics[2]
        assert metrics[0]["app_id"] == "orders-service"
        assert metrics[0]["operational_metrics_enabled"] is True
        assert metrics[0]["usage_metrics_enabled"] is True
        assert metrics[0]["command_count"] >= 1
        assert metrics[0]["nodes"]
    finally:
        _shutdown(cluster, *exporters)


def test_same_otel_exporter_registered_twice_is_called_twice():
    host, port = seed_host()
    exporter = _CountingExporter()
    cluster = None
    try:
        cluster = _connect(host, port)
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        cluster.add_exporter(exporter)
        assert cluster.exporters == (exporter, exporter)
        cluster.enable_metrics(MetricsPolicy(operational_enabled=True))
        _write(cluster)
        _wait(lambda: exporter.calls >= 2, "the duplicate registration was not called")
        assert exporter.calls % 2 == 0
        assert len(exporter.registry.client_keys()) == 1
        document = exporter.registry.current(exporter.registry.client_keys()[0])
        assert document["app_id"] == "orders-service"
    finally:
        _shutdown(cluster, exporter)


def test_two_clusters_each_register_a_different_exporter():
    host, port = seed_host()
    orders_exporter = _CountingExporter()
    billing_exporter = _CountingExporter()
    orders = billing = None
    try:
        orders = _connect(host, port, app_id="orders-service")
        billing = _connect(host, port, app_id="billing-service")
    except Exception as exc:
        for cluster in (orders, billing):
            if cluster is not None:
                cluster.close()
        orders_exporter.close()
        billing_exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        orders.add_exporter(orders_exporter)
        billing.add_exporter(billing_exporter)
        orders.enable_metrics(MetricsPolicy(operational_enabled=True))
        billing.enable_metrics(MetricsPolicy(operational_enabled=True))
        _write(orders)
        _write(billing)
        _wait(
            lambda: orders_exporter.calls >= 1 and billing_exporter.calls >= 1,
            "each cluster did not call its exporter",
        )
        assert orders_exporter.registry is not billing_exporter.registry
        assert orders_exporter.registry.client_keys() != billing_exporter.registry.client_keys()
        orders_document = orders_exporter.registry.current(
            orders_exporter.registry.client_keys()[0]
        )
        billing_document = billing_exporter.registry.current(
            billing_exporter.registry.client_keys()[0]
        )
        assert len(orders_exporter.registry.client_keys()) == 1
        assert len(billing_exporter.registry.client_keys()) == 1
        assert orders_document["app_id"] == "orders-service"
        assert billing_document["app_id"] == "billing-service"
    finally:
        for cluster in (orders, billing):
            if cluster is not None:
                cluster.disable_metrics()
                cluster.close()
        orders_exporter.close()
        billing_exporter.close()
