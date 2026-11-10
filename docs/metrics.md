# Exported metrics

How this package turns one client snapshot into OpenTelemetry series. The
exporter contract, dependencies, and install are in the
[repository README](../README.md). The field-by-field catalog, snapshot
shape, and translation rules for a Go or other client exporter are in
[reference.md](reference.md).

The snapshot is one document per client, the canonical document from the
Python client SDK on `stage`. The identity is `client_type`, `client_version`,
`cluster_name`, and `app_id`. Two clients that share those four values are
one series. The webapp sets a distinct `app_id` per client. Cluster-wide
fields sit on the document's `cluster` object.

Tier 0 is published whenever metrics are enabled. Operational series are
published only when `operational_metrics_enabled` is true. Usage series are
published only when `usage_metrics_enabled` is true. A missing flag omits
that tier.

Exported names are `aerospike_client_*`, from the common metric-name table.
Cluster series have no `cluster` segment: `aerospike_client_command_count`.
Node series use `aerospike_client_node_`, and namespace series use
`aerospike_client_namespace_`. The names already use underscores, so a
Prometheus exporter does not rewrite them. Counters still gain a `_total`
suffix. The Python scrape reader follows `PrometheusMetricReader` 0.66b1,
including cumulative temporality. Its constructor is documented in
[prometheus-reader.md](prometheus-reader.md).

The full catalog, units, and snapshot paths are in [reference.md](reference.md).
Every registered name:

| Metric | Kind | Tier | Snapshot path |
|---|---|---|---|
| `aerospike_client_recover_queue_size` | gauge | Tier 0 | `cluster.recover_queue.size` |
| `aerospike_client_invalid_node_count` | counter | Tier 0 | `cluster.nodes.invalid` |
| `aerospike_client_nodes_active` | gauge | Tier 0 | `cluster.nodes.active` |
| `aerospike_client_command_count` | counter | Operational | `cluster.command_count` |
| `aerospike_client_command_retries` | counter | Operational | `cluster.command_retries` |
| `aerospike_client_cpu_percent` | gauge | Operational | `cluster.cpu_percent` |
| `aerospike_client_memory_bytes` | gauge | Operational | `cluster.memory_bytes` |
| `aerospike_client_latency_bucket` | counter | Operational | `cluster.latency` for `read`, `write`, `batch`, `query` |
| `aerospike_client_node_connections_in_use` | gauge | Tier 0 | `nodes[].connections.in_use` |
| `aerospike_client_node_connections_in_pool` | gauge | Tier 0 | `nodes[].connections.in_pool` |
| `aerospike_client_node_connections_opened` | counter | Tier 0 | `nodes[].connections.opened` |
| `aerospike_client_node_connections_closed` | counter | Tier 0 | `nodes[].connections.closed` |
| `aerospike_client_node_connection_open_failure` | counter | Operational | `connections.open_failure` |
| `aerospike_client_node_connection_tls_handshake_failure` | counter | Operational | `connections.tls_handshake_failure` |
| `aerospike_client_node_connection_auth_failure` | counter | Operational | `connections.auth_failure` |
| `aerospike_client_node_connection_closed_error` | counter | Operational | `connections.closed_error` |
| `aerospike_client_node_connection_closed_idle` | counter | Operational | `connections.closed_idle` |
| `aerospike_client_node_connection_closed_node_removed` | counter | Operational | `connections.closed_node_removed` |
| `aerospike_client_node_conn_latency_bucket` | counter | Operational | `nodes[].latency.conn` |
| `aerospike_client_namespace_errors` | counter | Operational | `namespaces[].errors` |
| `aerospike_client_namespace_server_timeouts` | counter | Operational | `namespaces[].timeouts` |
| `aerospike_client_namespace_key_busy` | counter | Operational | `namespaces[].key_busy` |
| `aerospike_client_namespace_record_too_big` | counter | Operational | `namespaces[].record_too_big` |
| `aerospike_client_namespace_device_overload` | counter | Operational | `namespaces[].device_overload` |
| `aerospike_client_namespace_bytes_in` | counter | Operational | `namespaces[].bytes_in` |
| `aerospike_client_namespace_bytes_out` | counter | Operational | `namespaces[].bytes_out` |
| `aerospike_client_namespace_latency_bucket` | counter | Operational | `namespaces[].latency` for `read`, `write`, `batch`, `query` |
| `aerospike_client_feature_api_blocking` | counter | Usage | `usage["feature.api.blocking"]` |
| `aerospike_client_feature_api_deferred` | counter | Usage | `usage["feature.api.deferred"]` |
| `aerospike_client_feature_api_background` | counter | Usage | `usage["feature.api.background"]` |
| `aerospike_client_feature_shape_point` | counter | Usage | `usage["feature.shape.point"]` |
| `aerospike_client_feature_shape_batch` | counter | Usage | `usage["feature.shape.batch"]` |
| `aerospike_client_feature_shape_query` | counter | Usage | `usage["feature.shape.query"]` |
| `aerospike_client_feature_filter_ael` | counter | Usage | `usage["feature.filter.ael"]` |
| `aerospike_client_feature_filter_exp` | counter | Usage | `usage["feature.filter.exp"]` |

`aerospike_client_nodes_active` is the current cluster size. The common name
table has no row for `cluster.nodes.active`. Cluster connection totals on
`cluster.connections` are left on the snapshot. Pool gauges are the per-node
series.

Latency buckets are cumulative counters with an `le` label. The snapshot field
`latency_unit` is a label on each latency metric. A group whose `count` is
0 is omitted. An existing-node open, TLS, or auth failure stays on the node
(`aerospike_client_node_connection_open_failure` and the TLS and auth names),
read from `connections.open_failure`, `tls_handshake_failure`, and
`auth_failure`. Close reasons the stage client classifies are
`aerospike_client_node_connection_closed_error`,
`aerospike_client_node_connection_closed_idle`, and
`aerospike_client_node_connection_closed_node_removed`. Stage has no
`pool_shrink` counter.

`aerospike_client_node_conn_latency_bucket` is socket-open latency from
`nodes[].latency.conn`. Stage records pool-acquisition time on
`namespaces[].latency.conn` instead, and that group is not exported. Client
deadlines stay on `cluster.command_timeout_client`. The name table has no
cluster series for that counter, and stage does not split it per namespace,
so it is not published as `aerospike_client_namespace_client_timeouts`.

Labels on every series include `cluster`, `client_type`, `client_version`,
and `app_id`. Whether a client-object identifier should separate two clients
that share those four values is an open question for the client team. Node
series add `node`, `node_address`, and `node_port`. Namespace series also
add `namespace`. A webapp label that uses one of those names is dropped.
Exporter values win. `conn_type` is reserved and is not set: the Python SDK
snapshot has one connection pool.

`client_app_ip` is not a series label. In Kubernetes the default-route
address is the pod address, so the label would create one series per pod.
`client_app_ip()` still returns that address for a caller that wants it. A
webapp label of the same name is dropped and is not published.

A node that has left and is still absent is listed in `nodes_departed`.
Its final counters are published with the live nodes. The same node name in
both lists is published from `nodes` only. A node that leaves and rejoins
before the snapshot is built is only in `nodes`.

`export()` stores that one document and returns. The webapp's reader collects
it later. A slower reader publishes only the newest document. A cumulative
counter that is still on the new document keeps its latest absolute total.
A series that existed only on a skipped document is not published. That is
any stat, not only `nodes_departed`: a departed node, a gauge that moved and
came back, or any other label set that disappeared before the reader ran.
The client can also drain `nodes_departed` when `export()` returns, which is
before that collect.

Plan, not implemented: keep each document until a reader callback has
observed it. Emit a series that is only on an older uncollected document
once, from that document. Emit a series that is on the newest document from
the newest absolute value, so counters are not added twice. Agree with the
client team when `nodes_departed` is drained before building this.

Instrument units stay the OpenTelemetry strings already registered, such as
`{connection}`, `{command}`, `{operation}`, `%`, and `By`. The spec table
spells some of those as `connection`, `command`, `count`, and `bytes`. The
unit is part of the instrument identity, so changing it publishes a new
series. Plan, not implemented: agree the spelling with the client team
before changing a unit.

The metrics log file is the client SDK `MetricsWriter`. File mode stays
`exporter: file` and `report_dir`. This package does not ship a writer and
does not add a format field to the client configuration. The current file is
the positional `header(5)` log. The SDK metrics guide records which snapshot
fields that log carries, how a slow or failed write behaves, and the
`header(6)` layout the same writer will grow into.
