# Exported metrics

How this package turns one client snapshot into OpenTelemetry series. The
exporter contract, dependencies, and install are in the
[repository README](../README.md). The field-by-field catalog, snapshot
shape, and translation rules for a Go or other client exporter are in
[reference.md](reference.md).

The snapshot is one document per client. The identity is `client_type`,
`client_version`, `cluster_name`, and `app_id`. Two clients that share those
four values are one series. The webapp sets a distinct `app_id` per client.
There is no `cluster` object in the document. Cluster-wide fields sit on the
document next to `command_count`.

`client_timeouts` is only on each namespace object,
`nodes[].namespaces[].client_timeouts`. It is not repeated on the document.

Tier 0 is published whenever metrics are enabled. Operational series are
published only when `operational_metrics_enabled` is true. Usage series are
published only when `usage_metrics_enabled` is true. A missing flag omits
that tier.

Every metric is `aerospike.client.<context>.<stat>`. The context stays in the
name: `cluster`, `node`, `namespace`, or `feature`. For example,
`aerospike.client.cluster.command_count` and
`aerospike.client.node.connections_in_use`. A Prometheus exporter replaces
each dot with an underscore, so the same series is
`aerospike_client_<context>_<stat>` there.

| Metric | Kind | Tier | Where it comes from |
|---|---|---|---|
| `aerospike.client.cluster.invalid_node_count` | counter | Tier 0 | Lifetime invalid-node events. Not the current node count. |
| `aerospike.client.cluster.total_nodes` | gauge | Tier 0 | Current cluster size. |
| `aerospike.client.cluster.recover_queue_size` | gauge | Tier 0 | Connections waiting in the recovery queue. |
| `aerospike.client.cluster.delay_queue_timeout_count` | counter | Tier 0 | Stays 0 until the native snapshot reports it. |
| `aerospike.client.cluster.validation_open_failure` | counter | Operational | Seed validation that never becomes a node. Stays 0 until native reports it. |
| `aerospike.client.cluster.validation_tls_handshake_failure` | counter | Operational | Same, for TLS. |
| `aerospike.client.cluster.validation_auth_failure` | counter | Operational | Same, for authentication. |
| `aerospike.client.cluster.batch_partial_failure` | counter | Operational | Batch commands that finished with a partial failure. Stays 0 until native reports it. |
| `aerospike.client.cluster.cpu_percent` | gauge | Operational | Process CPU. |
| `aerospike.client.cluster.memory_bytes` | gauge | Operational | Process memory. |
| `aerospike.client.cluster.latency_bucket` | counter | Operational | Cluster whole-call latency. Label `latency_type` is `read`, `write`, `batch`, or `query`. |
| `aerospike.client.namespace.latency_bucket` | counter | Operational | Namespace RPC latency. Same `latency_type` label, plus the node labels and `ns`. |
| `aerospike.client.node.conn_latency_bucket` | counter | Operational | Connection-acquisition latency on a node. Label `latency_type` is `conn`. |
| `aerospike.client.namespace.client_timeouts` | counter | Operational | Client-side timeouts on that namespace. Stays 0 until native reports it. |
| `aerospike.client.feature.api_blocking` | counter | Usage | One counter per feature. The other usage names follow the same pattern: `aerospike.client.feature.api_deferred`, `aerospike.client.feature.api_background`, `aerospike.client.feature.shape_point`, `aerospike.client.feature.shape_batch`, `aerospike.client.feature.shape_query`, `aerospike.client.feature.object_mapping_read`, `aerospike.client.feature.object_mapping_write`, `aerospike.client.feature.filter_ael`, `aerospike.client.feature.filter_exp`. |

Latency buckets are cumulative counters with an `le` label. The snapshot field
`latency_unit` is a label on those three latency metrics. An existing-node
open, TLS, or auth failure stays on the node
(`aerospike.client.node.connection_open_failure` and the TLS and auth names).
That is a different series from the cluster validation counters above.

Labels on every series include `cluster`, `client_type`, `client_version`,
and `app_id`. Whether a client-object identifier should separate two clients
that share those four values is an open question for the client team. Node
series add `server_node_id`, `server_node_address`, and `server_node_port`.
Namespace series also add `ns`. Those names stay, because the shorter spec
names are generic enough to collide with application labels. A webapp label
that uses one of the exported names is dropped. Exporter values win. Revisit
the node and namespace names if the client or product team wants the spec
names.

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
