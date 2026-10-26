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
import socket
from typing import Any, Optional


class InstrumentKind(str, Enum):
    """How a canonical field is reported."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


# Snapshot label key to the name exported on every series. A key that is
# absent keeps its own name. The node id, address, port, and namespace are
# read off each node object and renamed here.
LABEL_MAP = {
    "cluster_name": "cluster",
    "client_type": "client_type",
    "client_version": "client_version",
    "app_id": "app_id",
    "node": "server_node_id",
    "node_address": "server_node_address",
    "node_port": "server_node_port",
    "namespace": "ns",
}

# Exported names a webapp label must not replace. Identity and the dimensions
# this package adds (server node id, namespace, bucket bound) occupy them.
RESERVED_LABELS = frozenset(
    {
        "cluster",
        "client_type",
        "client_version",
        "app_id",
        "server_node_id",
        "server_node_address",
        "server_node_port",
        "ns",
        "le",
        "feature",
        "conn_type",
    }
)

_IDENTITY = ("client_type", "client_version", "cluster_name", "app_id")
_CLIENT_APP_IP_LABEL = "client_app_ip"
_client_app_ip = ""
_NODE_LABELS = ("server_node_id", "server_node_address", "server_node_port")
_NAMESPACE_LABELS = _NODE_LABELS + ("ns",)


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
        "recover_queue_size": _gauge(
            "recover_queue_size",
            "aerospike_client_recover_queue_size",
            "Connections waiting in the recovery queue",
            "{connection}",
        ),
        "invalid_node_count": _gauge(
            "invalid_node_count",
            "aerospike_client_invalid_node_count",
            "Nodes the client currently treats as invalid",
            "{node}",
        ),
        "command_retries": _counter(
            "command_retries",
            "aerospike_client_retry_count",
            "Transaction retries",
            "{retry}",
        ),
        "total_nodes": _gauge(
            "total_nodes",
            "aerospike_client_total_nodes",
            "Current cluster node count",
            "{node}",
        ),
        "open_connections": _gauge(
            "open_connections",
            "aerospike_client_open_connections",
            "Connections currently open across the cluster",
            "{connection}",
        ),
        "cpu_percent": _gauge(
            "cpu_percent",
            "aerospike_client_cpu_percent",
            "Client process CPU utilization",
            "%",
        ),
        "memory_bytes": _gauge(
            "memory_bytes",
            "aerospike_client_memory_bytes",
            "Client process memory usage",
            "By",
        ),
        "command_count": _counter(
            "command_count",
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
        "node.connection_open_failure": _counter(
            "node.connection_open_failure",
            "aerospike_client_node_connection_open_failure",
            "Connection open failures on an existing node",
            "{error}",
            _NODE_LABELS,
        ),
        "node.connection_tls_handshake_failure": _counter(
            "node.connection_tls_handshake_failure",
            "aerospike_client_node_connection_tls_handshake_failure",
            "TLS handshake failures on an existing node",
            "{error}",
            _NODE_LABELS,
        ),
        "node.connection_auth_failure": _counter(
            "node.connection_auth_failure",
            "aerospike_client_node_connection_auth_failure",
            "Authentication failures on an existing node",
            "{error}",
            _NODE_LABELS,
        ),
        "namespace.errors": _counter(
            "namespace.errors",
            "aerospike_client_namespace_errors",
            "Namespace operation errors",
            "{error}",
            _NAMESPACE_LABELS,
        ),
        "namespace.server_timeouts": _counter(
            "namespace.server_timeouts",
            "aerospike_client_namespace_server_timeouts",
            "Namespace operation timeouts reported by the server",
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
    metrics["node.latency.conn"] = _counter(
        "node.latency.conn",
        "aerospike_client_node_latency_conn_bucket",
        "Cumulative node connection-acquisition latency observations by upper bound",
        "{operation}",
        _NODE_LABELS + ("le",),
    )
    for operation in ("read", "write", "batch", "query"):
        source = f"cluster.latency.{operation}"
        metrics[source] = _counter(
            source,
            f"aerospike_client_cluster_latency_{operation}_bucket",
            f"Cumulative whole-call {operation} latency observations by upper bound",
            "{operation}",
            ("le",),
        )
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

# The client records these only while MetricsPolicy.operational_enabled is on.
# The snapshot can still carry the last totals after the tier is turned off.
_OPERATIONAL_PREFIXES = ("cluster.latency.", "namespace.latency.")
_OPERATIONAL_SOURCES = frozenset(
    {
        "command_count",
        "command_retries",
        "node.connection_open_failure",
        "node.connection_tls_handshake_failure",
        "node.connection_auth_failure",
        "node.latency.conn",
        "namespace.errors",
        "namespace.server_timeouts",
        "namespace.key_busy",
        "namespace.record_too_big",
        "namespace.device_overload",
        "namespace.bytes_in",
        "namespace.bytes_out",
    }
)


def _tier_is_off(document: Mapping[str, Any], source: str) -> bool:
    """Whether this snapshot says not to publish ``source``.

    A missing flag does not hide the series. ``False`` does. Always-on gauges
    are published whenever the document is present.
    """
    if source == "usage":
        return document.get("usage_metrics_enabled") is False
    if source in _OPERATIONAL_SOURCES or source.startswith(_OPERATIONAL_PREFIXES):
        return document.get("operational_metrics_enabled") is False
    return False


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


def client_app_ip() -> str:
    """IPv4 address of the process exporting metrics.

    A UDP socket chooses the outbound interface and does not send a packet.
    The webapp can set the ``client_app_ip`` label itself; that value is kept.
    """
    global _client_app_ip
    if _client_app_ip:
        return _client_app_ip
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("192.0.2.1", 1))
            _client_app_ip = sock.getsockname()[0]
    except OSError:
        try:
            _client_app_ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            _client_app_ip = ""
    return _client_app_ip


def series_attributes(
    document: Mapping[str, Any],
    extra: Optional[Mapping[str, Any]] = None,
    label_map: Optional[Mapping[str, str]] = None,
) -> dict[str, str]:
    """Labels for one series.

    Identity is renamed through the global map. A webapp label that lands on
    a reserved name is dropped. ``client_app_ip`` is the webapp address and is on
    every series. ``extra`` holds dimensions for this point, such as ``server_node_id``
    or ``ns``, and those are renamed by the same map.
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
    if _CLIENT_APP_IP_LABEL not in attributes:
        app_ip = client_app_ip()
        if app_ip:
            attributes[_CLIENT_APP_IP_LABEL] = app_ip
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

    Latency histograms are already bucketed by the client. The OTel API has no
    observable histogram callback, so a compatible Prometheus-style
    histogram is emitted as cumulative observable counters with an ``le``
    label. Bucket boundaries are in the snapshot's latency unit.

    ``operational_metrics_enabled`` false omits the operational tier, and
    ``usage_metrics_enabled`` false omits feature usage. Always-on gauges are
    still yielded.
    """
    source = descriptor.source
    if _tier_is_off(document, source):
        return
    if source.startswith("cluster.latency."):
        group = source.rsplit(".", 1)[1]
        buckets = _latency_buckets(document.get("cluster_latencies"), group)
        yield from _bucket_points(document, buckets, {})
        return
    if source == "node.latency.conn":
        for node in _mappings(document.get("nodes")):
            histogram = node.get("conn_latency")
            buckets = (
                histogram.get("bucket_counts")
                if isinstance(histogram, Mapping) and histogram.get("type") == "conn"
                else None
            )
            yield from _bucket_points(document, buckets, _node_labels(node))
        return
    if source.startswith("namespace.latency."):
        group = source.rsplit(".", 1)[1]
        for node in _mappings(document.get("nodes")):
            node_labels = _node_labels(node)
            for namespace in _mappings(node.get("namespaces")):
                point = dict(node_labels)
                if namespace.get("namespace"):
                    point["ns"] = str(namespace["namespace"])
                yield from _bucket_points(
                    document,
                    _latency_buckets(namespace.get("latencies"), group),
                    point,
                )
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
                if namespace.get("namespace"):
                    point["ns"] = str(namespace["namespace"])
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
    number = _number(_dig(document, source))
    if number is not None:
        yield number, {}


def _mappings(value: Any) -> Iterator[Mapping[str, Any]]:
    for item in value or ():
        if isinstance(item, Mapping):
            yield item


def _node_labels(node: Mapping[str, Any]) -> dict[str, str]:
    labels: dict[str, str] = {}
    if node.get("node_name"):
        labels["server_node_id"] = str(node["node_name"])
    if node.get("node_address"):
        labels["server_node_address"] = str(node["node_address"])
    if node.get("node_port") is not None:
        labels["server_node_port"] = str(node["node_port"])
    return labels


def _latency_buckets(latencies: Any, group: str) -> Any:
    for latency in _mappings(latencies):
        if latency.get("type") == group:
            return latency.get("bucket_counts")
    return None


def _bucket_points(
    document: Mapping[str, Any],
    buckets: Any,
    labels: Mapping[str, str],
) -> Iterator[tuple[int, dict[str, str]]]:
    if not isinstance(buckets, (list, tuple)) or not buckets:
        return
    shift = _number(document.get("latency_shift"))
    base = 2 ** int(shift) if shift is not None else 2
    cumulative = 0
    upper_bound = 1
    for index, raw_count in enumerate(buckets):
        count = _number(raw_count)
        if count is None:
            continue
        cumulative += int(count)
        point = dict(labels)
        point["le"] = "+Inf" if index == len(buckets) - 1 else _format_latency_bound(upper_bound)
        yield cumulative, point
        upper_bound *= base


def _format_latency_bound(value: int) -> str:
    """Prometheus-compatible spelling for an integral latency upper bound."""
    return str(value)


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
