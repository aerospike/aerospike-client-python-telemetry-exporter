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
        "command_count": 4,
        "total_nodes": 1,
        "nodes": [
            {
                "node_name": "BB9",
                "node_address": "127.0.0.1",
                "node_port": 3000,
                "connections": {"opened": 2},
                "namespaces": [{"namespace": "test", "errors": 7}],
            }
        ],
    }


def test_one_label_rename_applies_to_every_metric():
    document = _document()
    label_map = {"region": "location", "cluster_name": "cluster"}
    command = CATALOG.by_source("command_count")
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
    assert error_labels["ns"] == "test"
    assert error_labels["server_node_id"] == "BB9"
    assert error_value == 7
    assert command.kind is InstrumentKind.COUNTER
    assert CATALOG.by_source("total_nodes").kind is InstrumentKind.GAUGE


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
    assert errors["ns"] == "test"
    document["labels"] = {**document["labels"], "client_app_ip": "203.0.113.10"}
    assert series_attributes(document)["client_app_ip"] == "203.0.113.10"


def test_latency_buckets_are_cumulative_observable_counters():
    document = _document()
    document["nodes"][0]["namespaces"][0]["latencies"] = [
        {"type": "read", "bucket_counts": [1, 2, 3]}
    ]
    descriptor = CATALOG.by_source("namespace.latency.read")
    assert descriptor.name == "aerospike_client_namespace_latency_read_bucket"
    assert "le" in descriptor.labels
    labels = {
        "server_node_id": "BB9",
        "server_node_address": "127.0.0.1",
        "server_node_port": "3000",
        "ns": "test",
    }
    assert list(iter_metric_points(document, descriptor)) == [
        (1, {**labels, "le": "1"}),
        (3, {**labels, "le": "2"}),
        (6, {**labels, "le": "+Inf"}),
    ]


def test_latency_shift_controls_histogram_upper_bounds():
    document = _document()
    document["latency_shift"] = 3
    document["nodes"][0]["namespaces"][0]["latencies"] = [
        {"type": "read", "bucket_counts": [1, 1, 1, 1]}
    ]
    descriptor = CATALOG.by_source("namespace.latency.read")
    assert [labels["le"] for _, labels in iter_metric_points(document, descriptor)] == [
        "1",
        "8",
        "64",
        "+Inf",
    ]


def test_cluster_and_node_latency_use_their_revised_scopes():
    document = _document()
    document["cluster_latencies"] = [{"type": "read", "bucket_counts": [2, 3], "count": 5}]
    document["nodes"][0]["conn_latency"] = {
        "type": "conn",
        "bucket_counts": [4, 1],
        "count": 5,
    }

    cluster = CATALOG.by_source("cluster.latency.read")
    node = CATALOG.by_source("node.latency.conn")

    assert list(iter_metric_points(document, cluster)) == [
        (2, {"le": "1"}),
        (5, {"le": "+Inf"}),
    ]
    assert list(iter_metric_points(document, node)) == [
        (
            4,
            {
                "server_node_id": "BB9",
                "server_node_address": "127.0.0.1",
                "server_node_port": "3000",
                "le": "1",
            },
        ),
        (
            5,
            {
                "server_node_id": "BB9",
                "server_node_address": "127.0.0.1",
                "server_node_port": "3000",
                "le": "+Inf",
            },
        ),
    ]


def test_existing_node_connection_failures_are_mapped():
    document = _document()
    document["nodes"][0]["connection_open_failure"] = 3
    descriptor = CATALOG.by_source("node.connection_open_failure")
    assert list(iter_metric_points(document, descriptor)) == [
        (
            3,
            {
                "server_node_id": "BB9",
                "server_node_address": "127.0.0.1",
                "server_node_port": "3000",
            },
        )
    ]


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
    for dropped in (
        "node",
        "namespace",
        "server_node_id",
        "ns",
        "feature",
        "le",
        "node_address",
        "node_port",
        "server_node_address",
        "server_node_port",
        "conn_type",
    ):
        assert dropped not in labels

    document["labels"] = ["environment:prod"]
    assert "environment" not in series_attributes(document)


def test_missing_names_omit_the_label_and_duplicate_names_both_yield():
    document = _document()
    document["nodes"] = [
        {
            "node_name": "",
            "node_address": "127.0.0.1",
            "node_port": 3000,
            "namespaces": [
                {"namespace": "", "errors": 1},
                {"namespace": "test", "errors": 2},
            ],
        },
        {
            "node_name": "BB9",
            "node_address": "10.0.0.1",
            "node_port": 3000,
            "connections": {"opened": 3},
        },
        {
            "node_name": "BB9",
            "node_address": "10.0.0.2",
            "node_port": 3000,
            "connections": {"opened": 4},
        },
    ]
    errors = list(iter_metric_points(document, CATALOG.by_source("namespace.errors")))
    blank, named = errors
    assert blank[0] == 1
    assert "ns" not in blank[1]
    assert "server_node_id" not in blank[1]
    assert named[1]["ns"] == "test"

    opened = list(iter_metric_points(document, CATALOG.by_source("node.connections.opened")))
    assert [value for value, _ in opened] == [3, 4]
    assert opened[0][1]["server_node_id"] == opened[1][1]["server_node_id"] == "BB9"
    assert opened[0][1]["server_node_address"] != opened[1][1]["server_node_address"]


def test_departed_nodes_rollups_and_non_numbers_are_omitted():
    document = _document()
    document["nodes"] = []
    document["nodes_departed"] = [
        {
            "node_name": "gone",
            "node_address": "10.0.0.9",
            "node_port": 3000,
            "connections": {"opened": 8},
        }
    ]
    document["command_count"] = "4"
    document["total_nodes"] = True
    document.pop("command_retries", None)

    opened = CATALOG.by_source("node.connections.opened")
    assert list(iter_metric_points(document, opened)) == []
    assert list(iter_metric_points(document, CATALOG.by_source("command_count"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("total_nodes"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("command_retries"))) == []


def test_unmapped_snapshot_fields_have_no_descriptor():
    for source in (
        "node.connections.open_failure",
        "node.error_rate",
        "delay_queue_timeout_count",
        "nodes_departed",
    ):
        with pytest.raises(KeyError):
            CATALOG.by_source(source)


def test_disabled_tiers_omit_their_series_and_keep_the_always_on_gauges():
    document = _document()
    document["operational_metrics_enabled"] = False
    document["usage_metrics_enabled"] = False
    document["command_retries"] = 3
    document["cpu_percent"] = 12.5
    document["usage"] = {"feature.api.blocking": 4}
    document["nodes"][0]["conn_latency"] = {"type": "conn", "bucket_counts": [1, 1]}
    document["nodes"][0]["connection_open_failure"] = 2

    assert list(iter_metric_points(document, CATALOG.by_source("command_count"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("command_retries"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("namespace.errors"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("namespace.latency.read"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("node.latency.conn"))) == []
    assert (
        list(iter_metric_points(document, CATALOG.by_source("node.connection_open_failure"))) == []
    )
    assert list(iter_metric_points(document, CATALOG.by_source("usage"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("total_nodes"))) == [(1, {})]
    assert list(iter_metric_points(document, CATALOG.by_source("cpu_percent"))) == [(12.5, {})]
    assert list(iter_metric_points(document, CATALOG.by_source("node.connections.opened"))) == [
        (
            2,
            {
                "server_node_id": "BB9",
                "server_node_address": "127.0.0.1",
                "server_node_port": "3000",
            },
        )
    ]

    document["operational_metrics_enabled"] = True
    assert list(iter_metric_points(document, CATALOG.by_source("command_count"))) == [(4, {})]
    assert list(iter_metric_points(document, CATALOG.by_source("usage"))) == []
    document["usage_metrics_enabled"] = True
    assert list(iter_metric_points(document, CATALOG.by_source("usage"))) == [
        (4, {"feature": "feature.api.blocking"})
    ]


def test_registry_keeps_the_latest_document_for_an_identity():
    registry = ClientSnapshotRegistry()
    key = "python-sdk\n1.0.0\nlocal\norders-service"
    registry.replace(key, {"labels": {"region": "us-west"}, "command_count": 1})
    registry.replace(key, {"labels": {"region": "eu"}, "command_count": 2})

    assert registry.client_keys() == (key,)
    assert registry.current(key)["labels"] == {"region": "eu"}
    assert registry.current(key)["command_count"] == 2
