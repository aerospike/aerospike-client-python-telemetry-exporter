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

"""Shared setup for periodic-export integration tests."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timedelta

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.metrics import MetricsExporterType, MetricsPolicy
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings

# The first scheduler tick waits one full interval. One second keeps the
# acceptance test on the real timer without a long wait.
EXPORT_INTERVAL_SECONDS = 1
CALLBACK_TIMEOUT_SECONDS = 8

os.environ.setdefault("AEROSPIKE_NAMESPACE", "test")


def seed_host() -> tuple[str, int]:
    """Return the configured Aerospike seed as ``(host, port)``."""
    raw = os.environ["AEROSPIKE_HOST"]
    host, separator, port = raw.rpartition(":")
    if not separator or not host or not port.isdigit():
        raise ValueError(f"AEROSPIKE_HOST must be host:port, got {raw!r}")
    return host, int(port)


def custom_metrics_settings() -> SystemSettings:
    """CUSTOM mode so registered exporters, and only those, receive the push."""
    return SystemSettings(
        metrics=MetricsSettings(
            exporter=MetricsExporterType.CUSTOM,
            export_interval=timedelta(seconds=EXPORT_INTERVAL_SECONDS),
        )
    )


def metrics_policy() -> MetricsPolicy:
    """Operational tier so generated traffic is visible on the snapshot."""
    return MetricsPolicy(operational_enabled=True)


def namespace() -> str:
    return os.environ["AEROSPIKE_NAMESPACE"]


def dataset():
    return DataSet.of(namespace(), "telemetry_exporter")


def cluster_section(document: Mapping) -> Mapping:
    """The ``cluster`` object on a stage canonical snapshot."""
    cluster = document.get("cluster")
    return cluster if isinstance(cluster, Mapping) else {}


def command_count(document: Mapping):
    """``cluster.command_count``, or ``None`` when the snapshot omits it."""
    return cluster_section(document).get("command_count")


def active_node_count(document: Mapping):
    """``cluster.nodes.active``, the current cluster size."""
    nodes = cluster_section(document).get("nodes")
    if isinstance(nodes, Mapping):
        return nodes.get("active")
    return None


def invalid_node_count(document: Mapping):
    """``cluster.nodes.invalid``."""
    nodes = cluster_section(document).get("nodes")
    if isinstance(nodes, Mapping):
        return nodes.get("invalid")
    return None


def namespace_latency_count(document: Mapping, group: str, namespace_name: str) -> int:
    """Sum ``count`` for one latency group across nodes in that namespace."""
    total = 0
    for node in document.get("nodes") or []:
        if not isinstance(node, Mapping):
            continue
        for namespace_doc in node.get("namespaces") or []:
            if not isinstance(namespace_doc, Mapping):
                continue
            if namespace_doc.get("name") != namespace_name:
                continue
            latency = namespace_doc.get("latency")
            if not isinstance(latency, Mapping):
                continue
            histogram = latency.get(group)
            if isinstance(histogram, Mapping):
                total += int(histogram.get("count") or 0)
    return total


def assert_same_cycle(left, right) -> None:
    """Two exporters observed one scheduler cycle.

    The snapshot stamps ``timestamp`` when it is built, so both translations
    of one window carry that time. A gap well under the export interval is
    the same cycle.
    """
    assert left.node_names == right.node_names
    assert left.command_count == right.command_count
    assert left.app_id == right.app_id
    left_at = datetime.fromisoformat(left.timestamp)
    right_at = datetime.fromisoformat(right.timestamp)
    assert abs(left_at - right_at) < timedelta(milliseconds=500)
