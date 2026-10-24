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

"""Metric names and the one label rename shared by every backend.

``METRIC_MAP`` names each snapshot field once. ``LABEL_MAP`` renames a label
for every metric. A backend supplies its own instruments and calls
:func:`series_attributes` and :func:`iter_metric_points` rather than keeping
a second map.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class InstrumentKind(str, Enum):
    """How a canonical field is reported."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


# Snapshot label key to the name exported on every series. A key that is
# absent keeps its own name. Node ``name`` and namespace ``name`` are not
# entries: those fields become ``node`` and ``namespace`` when they are read
# off their own object.
LABEL_MAP = {
    "cluster_name": "cluster",
    "client_type": "client_type",
    "client_version": "client_version",
    "app_id": "app_id",
}

# Exported names a webapp label must not replace. Identity and the dimensions
# this package adds (node, namespace, bucket bound) occupy them.
RESERVED_LABELS = frozenset(
    {
        "cluster",
        "client_type",
        "client_version",
        "app_id",
        "node",
        "node_address",
        "node_port",
        "namespace",
        "le",
        "feature",
        "conn_type",
    }
)

_IDENTITY = ("client_type", "client_version", "cluster_name", "app_id")
_NODE_LABELS = ("node", "node_address", "node_port")
_NAMESPACE_LABELS = _NODE_LABELS + ("namespace",)


@dataclass(frozen=True)
class MetricDescriptor:
    """One exported metric, independent of any backend."""

    name: str
    kind: InstrumentKind
    description: str
    unit: str = ""
    source: str = ""
    labels: tuple[str, ...] = ()


def _gauge(
    source: str,
    name: str,
    description: str,
    unit: str,
    labels: tuple[str, ...] = (),
) -> MetricDescriptor:
    return MetricDescriptor(name, InstrumentKind.GAUGE, description, unit, source, labels)


def _counter(
    source: str,
    name: str,
    description: str,
    unit: str,
    labels: tuple[str, ...] = (),
) -> MetricDescriptor:
    return MetricDescriptor(name, InstrumentKind.COUNTER, description, unit, source, labels)


def _metrics() -> dict[str, MetricDescriptor]:
    metrics = {
        "cluster.recover_queue.size": _gauge(
            "cluster.recover_queue.size",
            "aerospike_client_recover_queue_size",
            "Connections waiting in the recovery queue",
            "{connection}",
        ),
        "cluster.nodes.invalid": _gauge(
            "cluster.nodes.invalid",
            "aerospike_client_invalid_node_count",
            "Nodes the client currently treats as invalid",
            "{node}",
        ),
        "cluster.command_retries": _counter(
            "cluster.command_retries",
            "aerospike_client_retry_count",
            "Transaction retries",
            "{retry}",
        ),
        "cluster.nodes.active": _gauge(
            "cluster.nodes.active",
            "aerospike_client_total_nodes",
            "Current cluster node count",
            "{node}",
        ),
        "cluster.command_timeout_client": _counter(
            "cluster.command_timeout_client",
            "aerospike_client_command_timeout_client",
            "Commands the client gave up on",
            "{command}",
        ),
        "cluster.command_timeout_server": _counter(
            "cluster.command_timeout_server",
            "aerospike_client_command_timeout_server",
            "Commands the server reported as timed out",
            "{command}",
        ),
        "cluster.cpu_percent": _gauge(
            "cluster.cpu_percent",
            "aerospike_client_cpu_percent",
            "Client process CPU utilization",
            "%",
        ),
        "cluster.memory_bytes": _gauge(
            "cluster.memory_bytes",
            "aerospike_client_memory_bytes",
            "Client process memory usage",
            "By",
        ),
        "cluster.command_count": _counter(
            "cluster.command_count",
            "aerospike_client_command_count",
            "Commands issued by the client",
            "{command}",
        ),
        "node.connections.in_use": _gauge(
            "node.connections.in_use",
            "aerospike_client_node_connections_in_use",
            "Connections currently in use",
            "{connection}",
            _NODE_LABELS,
        ),
        "node.connections.in_pool": _gauge(
            "node.connections.in_pool",
            "aerospike_client_node_connections_in_pool",
            "Idle connections in the pool",
            "{connection}",
            _NODE_LABELS,
        ),
        "node.connections.opened": _counter(
            "node.connections.opened",
            "aerospike_client_node_connections_opened",
            "Connections opened",
            "{connection}",
            _NODE_LABELS,
        ),
        "node.connections.closed": _counter(
            "node.connections.closed",
            "aerospike_client_node_connections_closed",
            "Connections closed",
            "{connection}",
            _NODE_LABELS,
        ),
        "namespace.errors": _counter(
            "namespace.errors",
            "aerospike_client_namespace_errors",
            "Namespace operation errors",
            "{error}",
            _NAMESPACE_LABELS,
        ),
        "namespace.timeouts": _counter(
            "namespace.timeouts",
            "aerospike_client_namespace_timeouts",
            "Namespace operation timeouts",
            "{timeout}",
            _NAMESPACE_LABELS,
        ),
        "namespace.key_busy": _counter(
            "namespace.key_busy",
            "aerospike_client_namespace_key_busy",
            "Namespace key-busy errors",
            "{error}",
            _NAMESPACE_LABELS,
        ),
        "namespace.record_too_big": _counter(
            "namespace.record_too_big",
            "aerospike_client_namespace_record_too_big",
            "Namespace record-too-big errors",
            "{error}",
            _NAMESPACE_LABELS,
        ),
        "namespace.device_overload": _counter(
            "namespace.device_overload",
            "aerospike_client_namespace_device_overload",
            "Namespace device-overload errors",
            "{error}",
            _NAMESPACE_LABELS,
        ),
        "namespace.bytes_in": _counter(
            "namespace.bytes_in",
            "aerospike_client_namespace_bytes_in",
            "Bytes received for namespace operations",
            "By",
            _NAMESPACE_LABELS,
        ),
        "namespace.bytes_out": _counter(
            "namespace.bytes_out",
            "aerospike_client_namespace_bytes_out",
            "Bytes sent for namespace operations",
            "By",
            _NAMESPACE_LABELS,
        ),
        "usage": _counter(
            "usage",
            "aerospike_client_feature_usage",
            "SDK feature-usage counters",
            "{call}",
            ("feature",),
        ),
    }
    for operation in ("conn", "read", "write", "batch", "query"):
        source = f"namespace.latency.{operation}"
        metrics[source] = _counter(
            source,
            f"aerospike_client_namespace_latency_{operation}_bucket",
            f"Cumulative client-side {operation} latency observations by upper bound",
            "{operation}",
            _NAMESPACE_LABELS + ("le",),
        )
    return metrics


METRIC_MAP = _metrics()


class MetricCatalog:
    """Descriptors shared by backends."""

    def __init__(self, descriptors: Optional[Any] = None) -> None:
        self._descriptors: dict[str, MetricDescriptor] = {}
        self._by_source: dict[str, MetricDescriptor] = {}
        for descriptor in descriptors or ():
            self.register(descriptor)

    def register(self, descriptor: MetricDescriptor) -> None:
        """Add a descriptor. Repeating a name with a different definition fails."""
        current = self._descriptors.get(descriptor.name)
        if current is not None and current != descriptor:
            raise ValueError(f"metric {descriptor.name!r} is already registered")
        if descriptor.source:
            current_source = self._by_source.get(descriptor.source)
            if current_source is not None and current_source != descriptor:
                raise ValueError(f"metric source {descriptor.source!r} is already registered")
            self._by_source[descriptor.source] = descriptor
        self._descriptors[descriptor.name] = descriptor

    def get(self, name: str) -> MetricDescriptor:
        """Return the descriptor registered under its exported ``name``."""
        try:
            return self._descriptors[name]
        except KeyError:
            raise KeyError(name) from None

    def by_source(self, source: str) -> MetricDescriptor:
        """Return the descriptor for one snapshot field path."""
        try:
            return self._by_source[source]
        except KeyError:
            raise KeyError(source) from None

    def __iter__(self):
        return iter(self._descriptors.values())

    def __len__(self) -> int:
        return len(self._descriptors)


CATALOG = MetricCatalog(METRIC_MAP.values())


def rename_label(name: str, label_map: Optional[Mapping[str, str]] = None) -> str:
    """Return the exported label name. One map applies to every metric."""
    mapping = LABEL_MAP if label_map is None else label_map
    return mapping.get(name, name)


def series_attributes(
    document: Mapping[str, Any],
    extra: Optional[Mapping[str, Any]] = None,
    label_map: Optional[Mapping[str, str]] = None,
) -> dict[str, str]:
    """Labels for one series.

    Identity is renamed through the global map. A webapp label that lands on
    a reserved name is dropped. ``extra`` holds dimensions for this point,
    such as ``node`` or ``namespace``, and those are renamed by the same map.
    """
    attributes: dict[str, str] = {}
    for source in _IDENTITY:
        value = document.get(source, "")
        if value is None or value == "":
            continue
        attributes[rename_label(source, label_map)] = str(value)
    labels = document.get("labels") or {}
    if isinstance(labels, Mapping):
        for key, value in labels.items():
            exported = rename_label(str(key), label_map)
            if exported in RESERVED_LABELS or exported in attributes:
                continue
            attributes[exported] = str(value)
    if extra:
        for key, value in extra.items():
            if value is None or value == "":
                continue
            attributes[rename_label(str(key), label_map)] = str(value)
    return attributes


def iter_metric_points(
    document: Mapping[str, Any], descriptor: MetricDescriptor
) -> Iterator[tuple[int | float, dict[str, str]]]:
    """Yield ``(value, extra labels)`` for one descriptor.

    Latency bucket bounds are not computed here. Those descriptors are named
    in the catalog, and a backend emits them once it can label each bucket.
    """
    source = descriptor.source
    if source.startswith("namespace.latency."):
        return
    if source == "usage":
        usage = document.get("usage") or {}
        if isinstance(usage, Mapping):
            for feature, count in usage.items():
                number = _number(count)
                if number is not None:
                    yield number, {"feature": str(feature)}
        return
    if source.startswith("namespace."):
        field = source.split(".", 1)[1]
        for node in _mappings(document.get("nodes")):
            extra = _node_labels(node)
            for namespace in _mappings(node.get("namespaces")):
                number = _number(namespace.get(field)) if field in namespace else None
                if number is None:
                    continue
                point = dict(extra)
                if namespace.get("name"):
                    point["namespace"] = str(namespace["name"])
                yield number, point
        return
    if source.startswith("node."):
        field_path = source.split(".", 1)[1]
        for node in _mappings(document.get("nodes")):
            number = _number(_dig(node, field_path))
            if number is None:
                continue
            yield number, _node_labels(node)
        return
    if source.startswith("cluster."):
        number = _number(_dig(document.get("cluster") or {}, source.split(".", 1)[1]))
        if number is not None:
            yield number, {}


def _mappings(value: Any) -> Iterator[Mapping[str, Any]]:
    for item in value or ():
        if isinstance(item, Mapping):
            yield item


def _node_labels(node: Mapping[str, Any]) -> dict[str, str]:
    labels: dict[str, str] = {}
    if node.get("name"):
        labels["node"] = str(node["name"])
    if node.get("address"):
        labels["node_address"] = str(node["address"])
    if node.get("port") is not None:
        labels["node_port"] = str(node["port"])
    return labels


def _dig(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    return None
