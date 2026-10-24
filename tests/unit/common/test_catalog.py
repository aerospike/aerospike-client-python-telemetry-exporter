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

"""The shared metric map and the one global label rename."""

import pytest

from aerospike_client_telemetry.common import (
    CATALOG,
    ClientSnapshotRegistry,
    InstrumentKind,
    iter_metric_points,
    series_attributes,
)


def _document():
    return {
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "cluster_name": "local",
        "app_id": "orders-service",
        "labels": {"environment": "prod", "cluster_name": "spoofed", "region": "us-west"},
        "cluster": {"command_count": 4, "nodes": {"active": 1}},
        "nodes": [
            {
                "name": "BB9",
                "address": "127.0.0.1",
                "port": 3000,
                "connections": {"opened": 2},
                "namespaces": [{"name": "test", "errors": 7}],
            }
        ],
    }


def test_one_label_rename_applies_to_every_metric():
    document = _document()
    label_map = {"region": "location", "cluster_name": "cluster"}
    command = CATALOG.by_source("cluster.command_count")
    errors = CATALOG.by_source("namespace.errors")

    command_labels = series_attributes(document, label_map=label_map)
    error_value, extra = next(iter_metric_points(document, errors))
    error_labels = series_attributes(document, extra, label_map=label_map)

    assert command_labels["cluster"] == "local"
    assert command_labels["location"] == "us-west"
    assert "region" not in command_labels
    assert "cluster_name" not in command_labels
    assert error_labels["cluster"] == command_labels["cluster"]
    assert error_labels["location"] == command_labels["location"]
    assert error_labels["namespace"] == "test"
    assert error_labels["node"] == "BB9"
    assert error_value == 7
    assert command.kind is InstrumentKind.COUNTER
    assert CATALOG.by_source("cluster.nodes.active").kind is InstrumentKind.GAUGE


def test_client_app_ip_is_on_every_series(monkeypatch):
    monkeypatch.setattr(
        "aerospike_client_telemetry.common.catalog.client_app_ip",
        lambda: "198.51.100.8",
    )
    document = _document()
    command = series_attributes(document)
    _, extra = next(iter_metric_points(document, CATALOG.by_source("namespace.errors")))
    errors = series_attributes(document, extra)
    assert command["client_app_ip"] == "198.51.100.8"
    assert errors["client_app_ip"] == command["client_app_ip"]
    assert errors["namespace"] == "test"
    document["labels"] = {**document["labels"], "client_app_ip": "203.0.113.10"}
    assert series_attributes(document)["client_app_ip"] == "203.0.113.10"


def test_latency_names_are_in_the_catalog_without_bucket_values():
    document = _document()
    document["nodes"][0]["namespaces"][0]["latency"] = {"read": [1, 2, 3]}
    descriptor = CATALOG.by_source("namespace.latency.read")
    assert descriptor.name == "aerospike_client_namespace_latency_read_bucket"
    assert "le" in descriptor.labels
    assert list(iter_metric_points(document, descriptor)) == []


def test_reserved_and_non_mapping_labels_are_dropped():
    document = _document()
    document["labels"] = {
        "environment": "prod",
        "cluster": "spoofed-cluster",
        "node": "spoofed-node",
        "namespace": "spoofed-namespace",
        "app_id": "spoofed-app",
        "feature": "spoofed-feature",
        "le": "spoofed-bound",
        "client_type": "spoofed-type",
        "client_version": "spoofed-version",
        "node_address": "spoofed-address",
        "node_port": "spoofed-port",
        "conn_type": "spoofed-conn",
    }
    labels = series_attributes(document)
    assert labels["environment"] == "prod"
    assert labels["cluster"] == "local"
    assert labels["app_id"] == "orders-service"
    assert labels["client_type"] == "python-sdk"
    assert labels["client_version"] == "1.0.0"
    for dropped in ("node", "namespace", "feature", "le", "node_address", "node_port", "conn_type"):
        assert dropped not in labels

    document["labels"] = ["environment:prod"]
    assert "environment" not in series_attributes(document)


def test_missing_names_omit_the_label_and_duplicate_names_both_yield():
    document = _document()
    document["nodes"] = [
        {
            "name": "",
            "address": "127.0.0.1",
            "port": 3000,
            "namespaces": [{"name": "", "errors": 1}, {"name": "test", "errors": 2}],
        },
        {
            "name": "BB9",
            "address": "10.0.0.1",
            "port": 3000,
            "connections": {"opened": 3},
        },
        {
            "name": "BB9",
            "address": "10.0.0.2",
            "port": 3000,
            "connections": {"opened": 4},
        },
    ]
    errors = list(iter_metric_points(document, CATALOG.by_source("namespace.errors")))
    blank, named = errors
    assert blank[0] == 1
    assert "namespace" not in blank[1]
    assert "node" not in blank[1]
    assert named[1]["namespace"] == "test"

    opened = list(iter_metric_points(document, CATALOG.by_source("node.connections.opened")))
    assert [value for value, _ in opened] == [3, 4]
    assert opened[0][1]["node"] == opened[1][1]["node"] == "BB9"
    assert opened[0][1]["node_address"] != opened[1][1]["node_address"]


def test_departed_nodes_rollups_and_non_numbers_are_omitted():
    document = _document()
    document["nodes"] = []
    document["nodes_departed"] = [
        {"name": "gone", "address": "10.0.0.9", "port": 3000, "connections": {"opened": 8}}
    ]
    document["cluster"]["connections"] = {"opened": 11}
    document["cluster"]["command_count"] = "4"
    document["cluster"]["nodes"]["active"] = True
    document["cluster"].pop("command_retries", None)

    opened = CATALOG.by_source("node.connections.opened")
    assert list(iter_metric_points(document, opened)) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_count"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.nodes.active"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_retries"))) == []


def test_unmapped_snapshot_fields_have_no_descriptor():
    for source in (
        "node.connections.open_failure",
        "node.error_rate",
        "cluster.connections.opened",
        "nodes_departed",
    ):
        with pytest.raises(KeyError):
            CATALOG.by_source(source)


def test_registry_keeps_the_latest_document_for_an_identity():
    registry = ClientSnapshotRegistry()
    key = "python-sdk\n1.0.0\nlocal\norders-service"
    registry.replace(key, {"labels": {"region": "us-west"}, "cluster": {"command_count": 1}})
    registry.replace(key, {"labels": {"region": "eu"}, "cluster": {"command_count": 2}})

    assert registry.client_keys() == (key,)
    assert registry.current(key)["labels"] == {"region": "eu"}
    assert registry.current(key)["cluster"]["command_count"] == 2
