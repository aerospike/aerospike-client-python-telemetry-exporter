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
        "operational_metrics_enabled": True,
        "usage_metrics_enabled": True,
        "labels": {"environment": "prod", "cluster_name": "spoofed", "region": "us-west"},
        "cluster": {
            "command_count": 4,
            "command_retries": 1,
            "nodes": {"active": 1, "invalid": 0},
            "recover_queue": {"size": 0},
            "connections": {"in_use": 1, "in_pool": 0, "opened": 2, "closed": 0},
        },
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
    assert CATALOG.by_source("cluster.nodes.invalid").kind is InstrumentKind.COUNTER
    assert CATALOG.by_source("cluster.nodes.active").kind is InstrumentKind.GAUGE
    assert CATALOG.by_source("cluster.recover_queue.size").kind is InstrumentKind.GAUGE


def test_client_app_ip_is_not_a_series_label():
    document = _document()
    command = series_attributes(document)
    _, extra = next(iter_metric_points(document, CATALOG.by_source("namespace.errors")))
    errors = series_attributes(document, extra)
    assert "client_app_ip" not in command
    assert "client_app_ip" not in errors
    assert errors["namespace"] == "test"
    document["labels"] = {**document["labels"], "client_app_ip": "203.0.113.10"}
    assert "client_app_ip" not in series_attributes(document)


def test_latency_buckets_are_cumulative_observable_counters():
    document = _document()
    document["nodes"][0]["namespaces"][0]["latency"] = {
        "read": {"buckets": [1, 2, 3], "count": 6}
    }
    document["latency_unit"] = "milliseconds"
    descriptor = CATALOG.by_source("namespace.latency")
    assert descriptor.name == "aerospike_client_namespace_latency_bucket"
    assert "le" in descriptor.labels
    assert "latency_type" in descriptor.labels
    assert "latency_unit" in descriptor.labels
    labels = {
        "node": "BB9",
        "node_address": "127.0.0.1",
        "node_port": "3000",
        "namespace": "test",
        "latency_type": "read",
        "latency_unit": "milliseconds",
    }
    assert list(iter_metric_points(document, descriptor)) == [
        (1, {**labels, "le": "1"}),
        (3, {**labels, "le": "2"}),
        (6, {**labels, "le": "+Inf"}),
    ]


def test_latency_shift_controls_histogram_upper_bounds():
    document = _document()
    document["latency_shift"] = 3
    document["nodes"][0]["namespaces"][0]["latency"] = {
        "read": {"buckets": [1, 1, 1, 1], "count": 4}
    }
    descriptor = CATALOG.by_source("namespace.latency")
    assert [labels["le"] for _, labels in iter_metric_points(document, descriptor)] == [
        "1",
        "8",
        "64",
        "+Inf",
    ]


def test_cluster_and_namespace_latency_follow_the_stage_document():
    document = _document()
    document["cluster"]["latency"] = {
        "read": {"buckets": [2, 3], "count": 5},
        "query": {"buckets": [0, 0], "count": 0},
    }
    # Stage stores pool-acquisition time on the namespace. That is not
    # socket-open latency, so it does not become a namespace or node series.
    document["nodes"][0]["namespaces"][0]["latency"] = {
        "conn": {"buckets": [4, 1], "count": 5},
        "read": {"buckets": [1, 0], "count": 1},
    }
    document["nodes"][0]["latency"] = {"conn": {"buckets": [4, 1], "count": 5}}

    cluster = CATALOG.by_source("cluster.latency")
    namespace = CATALOG.by_source("namespace.latency")
    node_conn = CATALOG.by_source("node.latency.conn")
    assert cluster.name == "aerospike_client_latency_bucket"
    assert namespace.name == "aerospike_client_namespace_latency_bucket"
    assert node_conn.name == "aerospike_client_node_conn_latency_bucket"

    assert list(iter_metric_points(document, cluster)) == [
        (2, {"latency_type": "read", "le": "1"}),
        (5, {"latency_type": "read", "le": "+Inf"}),
    ]
    read = {
        "node": "BB9",
        "node_address": "127.0.0.1",
        "node_port": "3000",
        "namespace": "test",
        "latency_type": "read",
    }
    assert list(iter_metric_points(document, namespace)) == [
        (1, {**read, "le": "1"}),
        (1, {**read, "le": "+Inf"}),
    ]
    conn = {
        "node": "BB9",
        "node_address": "127.0.0.1",
        "node_port": "3000",
        "latency_type": "conn",
    }
    assert list(iter_metric_points(document, node_conn)) == [
        (4, {**conn, "le": "1"}),
        (5, {**conn, "le": "+Inf"}),
    ]


def test_existing_node_connection_failures_are_mapped():
    document = _document()
    document["nodes"][0]["connections"]["open_failure"] = 3
    document["nodes"][0]["connections"]["closed_error"] = 1
    node = {
        "node": "BB9",
        "node_address": "127.0.0.1",
        "node_port": "3000",
    }
    descriptor = CATALOG.by_source("node.connections.open_failure")
    assert list(iter_metric_points(document, descriptor)) == [(3, node)]
    closed = CATALOG.by_source("node.connections.closed_error")
    assert closed.name == "aerospike_client_node_connection_closed_error"
    assert list(iter_metric_points(document, closed)) == [(1, node)]


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
        "latency_type": "spoofed-type",
        "latency_unit": "spoofed-unit",
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
        "feature",
        "le",
        "latency_type",
        "latency_unit",
        "node_address",
        "node_port",
        "conn_type",
    ):
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
            "namespaces": [
                {"name": "", "errors": 1},
                {"name": "test", "errors": 2},
            ],
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


def test_departed_nodes_publish_their_final_counters_once():
    document = _document()
    document["nodes"][0]["namespaces"][0]["latency"] = {
        "read": {"buckets": [1, 0], "count": 1}
    }
    document["nodes_departed"] = [
        {
            "name": "BB9",
            "address": "10.9.9.9",
            "port": 3000,
            "connections": {"opened": 99},
            "namespaces": [{"name": "test", "errors": 99}],
        },
        {
            "name": "gone",
            "address": "10.0.0.9",
            "port": 3000,
            "connections": {"opened": 8},
            "latency": {"conn": {"buckets": [2, 1], "count": 3}},
            "namespaces": [
                {
                    "name": "test",
                    "errors": 3,
                    "latency": {"read": {"buckets": [4, 0], "count": 4}},
                }
            ],
        },
    ]

    opened = list(iter_metric_points(document, CATALOG.by_source("node.connections.opened")))
    assert [value for value, _ in opened] == [2, 8]
    assert opened[0][1]["node"] == "BB9"
    assert opened[0][1]["node_address"] == "127.0.0.1"
    assert opened[1][1]["node"] == "gone"

    errors = list(iter_metric_points(document, CATALOG.by_source("namespace.errors")))
    assert [(value, point["node"]) for value, point in errors] == [
        (7, "BB9"),
        (3, "gone"),
    ]

    namespace_latency = list(
        iter_metric_points(document, CATALOG.by_source("namespace.latency"))
    )
    assert {point["node"] for _, point in namespace_latency} == {"BB9", "gone"}
    assert {point["latency_type"] for _, point in namespace_latency} == {"read"}
    conn_latency = list(iter_metric_points(document, CATALOG.by_source("node.latency.conn")))
    assert {point["node"] for _, point in conn_latency} == {"gone"}


def test_non_numbers_are_omitted():
    document = _document()
    document["cluster"]["command_count"] = "4"
    document["cluster"]["nodes"]["active"] = True
    document["cluster"].pop("command_retries")

    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_count"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.nodes.active"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_retries"))) == []


def test_unmapped_snapshot_fields_have_no_descriptor():
    for source in (
        "node.error_rate",
        "node.connections.pool_exhausted",
        "cluster.command_timeout_client",
        "cluster.connections.in_use",
        "nodes_departed",
    ):
        with pytest.raises(KeyError):
            CATALOG.by_source(source)


def test_missing_tier_flags_omit_operational_and_usage_series():
    document = _document()
    document.pop("operational_metrics_enabled")
    document.pop("usage_metrics_enabled")
    document["usage"] = {"feature.api.blocking": 4}
    document["cluster"]["command_retries"] = 3

    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_count"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("namespace.errors"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("usage.feature.api.blocking"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.nodes.active"))) == [
        (1, {})
    ]
    assert list(iter_metric_points(document, CATALOG.by_source("node.connections.opened"))) == [
        (
            2,
            {
                "node": "BB9",
                "node_address": "127.0.0.1",
                "node_port": "3000",
            },
        )
    ]


def test_disabled_tiers_omit_their_series_and_keep_the_always_on_gauges():
    document = _document()
    document["operational_metrics_enabled"] = False
    document["usage_metrics_enabled"] = False
    document["cluster"]["command_retries"] = 3
    document["cluster"]["cpu_percent"] = 12.5
    document["usage"] = {"feature.api.blocking": 4}
    document["nodes"][0]["latency"] = {"conn": {"buckets": [1, 1], "count": 2}}
    document["nodes"][0]["connections"]["open_failure"] = 2

    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_count"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_retries"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("namespace.errors"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.latency"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("namespace.latency"))) == []
    assert (
        list(iter_metric_points(document, CATALOG.by_source("node.connections.open_failure")))
        == []
    )
    assert list(iter_metric_points(document, CATALOG.by_source("usage.feature.api.blocking"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.nodes.active"))) == [
        (1, {})
    ]
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.cpu_percent"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.memory_bytes"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("node.latency.conn"))) == []
    assert list(iter_metric_points(document, CATALOG.by_source("node.connections.opened"))) == [
        (
            2,
            {
                "node": "BB9",
                "node_address": "127.0.0.1",
                "node_port": "3000",
            },
        )
    ]

    document["operational_metrics_enabled"] = True
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.command_count"))) == [
        (4, {})
    ]
    assert list(iter_metric_points(document, CATALOG.by_source("cluster.cpu_percent"))) == [
        (12.5, {})
    ]
    conn = {
        "node": "BB9",
        "node_address": "127.0.0.1",
        "node_port": "3000",
        "latency_type": "conn",
    }
    assert list(iter_metric_points(document, CATALOG.by_source("node.latency.conn"))) == [
        (1, {**conn, "le": "1"}),
        (2, {**conn, "le": "+Inf"}),
    ]
    assert list(iter_metric_points(document, CATALOG.by_source("usage.feature.api.blocking"))) == []
    document["usage_metrics_enabled"] = True
    assert list(iter_metric_points(document, CATALOG.by_source("usage.feature.api.blocking"))) == [
        (4, {})
    ]
    assert (
        CATALOG.by_source("usage.feature.api.blocking").name
        == "aerospike_client_feature_api_blocking"
    )


def test_registry_keeps_the_latest_document_for_an_identity():
    registry = ClientSnapshotRegistry()
    key = "python-sdk\n1.0.0\nlocal\norders-service"
    registry.replace(key, {"labels": {"region": "us-west"}, "command_count": 1})
    registry.replace(key, {"labels": {"region": "eu"}, "command_count": 2})

    assert registry.client_keys() == (key,)
    assert registry.current(key)["labels"] == {"region": "eu"}
    assert registry.current(key)["command_count"] == 2
