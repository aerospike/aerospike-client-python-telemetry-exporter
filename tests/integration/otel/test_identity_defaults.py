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

"""Defaults the client fills when the webapp omits an identity method.

``app_id()`` and ``cluster_name()`` are optional. The SDK still stamps
``client_type`` and ``client_version``. ``MetricsPolicy.labels`` defaults to
an empty map. Two clients that both land on ``app_id`` ``not-set`` share one
registry entry. A client that sets a username and no ``app_id`` reports
that username.
"""

from __future__ import annotations

import time

import pytest

import aerospike_sdk
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


def _exporter() -> OtelExporter:
    return OtelExporter(RecordingProvider())


def _connect(host: str, port: int, *, app_id: str | None = None):
    definition = ClusterDefinition(host, port).with_system_settings(
        custom_metrics_settings()
    )
    if app_id is not None:
        definition = definition.app_id(app_id)
    return definition.connect()


def _enable(cluster, **policy) -> None:
    cluster.enable_metrics(MetricsPolicy(operational_enabled=True, **policy))


def _write(cluster, set_name: str) -> None:
    session = cluster.create_session()
    session.upsert(DataSet.of(namespace(), set_name).id(1)).put({"n": 1}).execute()


def _documents(exporter: OtelExporter) -> dict[str, dict]:
    return {
        key: exporter.registry.current(key)
        for key in exporter.registry.client_keys()
    }


def _wait(exporter: OtelExporter, predicate, message: str) -> dict[str, dict]:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        documents = _documents(exporter)
        if predicate(documents):
            return documents
        time.sleep(0.05)
    raise AssertionError(message)


def _shutdown(exporter: OtelExporter, *clusters) -> None:
    for cluster in clusters:
        if cluster is None:
            continue
        cluster.disable_metrics()
        cluster.close()
    exporter.close()


def test_omitted_app_id_is_not_set():
    host, port = seed_host()
    exporter = _exporter()
    cluster = None
    try:
        cluster = _connect(host, port)
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _enable(cluster)
        _write(cluster, "default_app_id")
        documents = _wait(
            exporter,
            lambda docs: any(doc.get("app_id") == "not-set" for doc in docs.values()),
            "periodic scheduler did not call export()",
        )
        document = next(iter(documents.values()))
        assert document["app_id"] == "not-set"
        assert list(documents) == [
            "\n".join(
                (
                    document["client_type"],
                    document["client_version"],
                    document["cluster_name"],
                    "not-set",
                )
            )
        ]
    finally:
        _shutdown(exporter, cluster)


def test_omitted_app_id_uses_the_username():
    host, port = seed_host()
    exporter = _exporter()
    cluster = None
    try:
        cluster = (
            ClusterDefinition(host, port)
            .with_system_settings(custom_metrics_settings())
            .with_native_credentials("orders-user", "secret")
            .connect()
        )
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _enable(cluster)
        _write(cluster, "username_app_id")
        documents = _wait(
            exporter,
            lambda docs: any(doc.get("app_id") == "orders-user" for doc in docs.values()),
            "username was not used as app_id",
        )
        document = next(doc for doc in documents.values() if doc.get("app_id") == "orders-user")
        assert document["app_id"] == "orders-user"
        assert "orders-user" in next(iter(documents))
    finally:
        _shutdown(exporter, cluster)


def test_omitted_cluster_name_is_the_server_name():
    host, port = seed_host()
    exporter = _exporter()
    cluster = None
    try:
        cluster = _connect(host, port, app_id="cluster-name-default")
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _enable(cluster)
        _write(cluster, "default_cluster_name")
        documents = _wait(
            exporter,
            lambda docs: any(doc.get("app_id") == "cluster-name-default" for doc in docs.values()),
            "periodic scheduler did not call export()",
        )
        document = next(iter(documents.values()))
        assert cluster.cluster_name
        assert document["cluster_name"] == cluster.cluster_name
    finally:
        _shutdown(exporter, cluster)


def test_sdk_fills_client_type_and_version():
    host, port = seed_host()
    exporter = _exporter()
    cluster = None
    try:
        cluster = _connect(host, port, app_id="sdk-identity")
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _enable(cluster)
        _write(cluster, "sdk_identity")
        documents = _wait(
            exporter,
            lambda docs: any(doc.get("app_id") == "sdk-identity" for doc in docs.values()),
            "periodic scheduler did not call export()",
        )
        document = next(iter(documents.values()))
        assert document["client_type"] == "python-sdk"
        assert document["client_version"] == aerospike_sdk.__version__
        assert document["client_version"] != "0.0.0"
    finally:
        _shutdown(exporter, cluster)


def test_omitted_labels_are_empty():
    host, port = seed_host()
    exporter = _exporter()
    cluster = None
    try:
        cluster = _connect(host, port, app_id="labels-default")
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        _enable(cluster)
        _write(cluster, "default_labels")
        documents = _wait(
            exporter,
            lambda docs: any(doc.get("app_id") == "labels-default" for doc in docs.values()),
            "periodic scheduler did not call export()",
        )
        document = next(iter(documents.values()))
        assert document["labels"] == {}
    finally:
        _shutdown(exporter, cluster)


def test_two_clients_using_the_default_app_id_keep_one_snapshot():
    host, port = seed_host()
    exporter = _exporter()
    first = second = None
    try:
        first = _connect(host, port)
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        first.add_exporter(exporter)
        _enable(first, labels={"owner": "alpha"})
        _write(first, "shared_default_a")
        _wait(
            exporter,
            lambda docs: any(doc.get("labels") == {"owner": "alpha"} for doc in docs.values()),
            "first client was not exported",
        )

        second = _connect(host, port)
        second.add_exporter(exporter)
        _enable(second, labels={"owner": "beta"})
        _write(second, "shared_default_b")
        documents = _wait(
            exporter,
            lambda docs: any(doc.get("labels") == {"owner": "beta"} for doc in docs.values()),
            "second client did not replace the shared snapshot",
        )
        assert len(documents) == 1
        document = next(iter(documents.values()))
        assert document["app_id"] == "not-set"
        assert document["labels"] == {"owner": "beta"}
    finally:
        _shutdown(exporter, first, second)


def test_two_clients_with_different_app_ids_keep_both_snapshots():
    host, port = seed_host()
    exporter = _exporter()
    orders = billing = None
    try:
        orders = _connect(host, port, app_id="orders-service")
        billing = _connect(host, port, app_id="billing-service")
    except Exception as exc:
        _shutdown(exporter, orders, billing)
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        for cluster, set_name in ((orders, "orders_client"), (billing, "billing_client")):
            cluster.add_exporter(exporter)
            _enable(cluster)
            _write(cluster, set_name)
        documents = _wait(
            exporter,
            lambda docs: {doc.get("app_id") for doc in docs.values()}
            >= {"orders-service", "billing-service"},
            "both clients were not exported",
        )
        assert len(documents) == 2
        assert {doc["app_id"] for doc in documents.values()} == {
            "orders-service",
            "billing-service",
        }
    finally:
        _shutdown(exporter, orders, billing)
