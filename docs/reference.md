# Implementation reference

Language-neutral rules for another client exporter, such as Go. The narrative
and the Python package contract are in [metrics.md](metrics.md) and the
[repository README](../README.md). This page is the catalog, the snapshot
shape, and the translation those series use.

The application owns the meter provider and every reader. The exporter stores
the latest canonical document per client and returns. Observable instruments
read that store when a reader collects. The exporter does not open OTLP and
does not choose HTTP or gRPC.

Register one observable instrument per row below, on the meter
`aerospike.client`. A counter row is an observable counter. A gauge row is an
observable gauge. The callback reports the absolute value from the latest
document. A histogram instrument is not used. Latency is the cumulative
counters in this catalog.

The Python client pushes the document through `export()`. Another language
uses that client's external-exporter hook and the same document.

## Store

Keep one document per identity. The key is `client_type`, `client_version`,
`cluster_name`, and `app_id`, in that order. Two clients that share those
four values replace one document, so they are one series. The application
sets a distinct `app_id` per client when the series must stay separate.

A collect walks every stored document. Callbacks can run on a reader thread
while `export()` replaces a document, so the replace and the read share one
lock. `export()` copies the canonical document and drops the snapshot object.

A missing or non-numeric value is omitted. Booleans are omitted. A present
`0` is published. Operational series are published only when
`operational_metrics_enabled` is true. Usage series are published only when
`usage_metrics_enabled` is true. Any other flag value, including a missing
flag, omits that tier. Tier 0 is published whenever the document is present.

## Snapshot

Cluster fields sit on the document. There is no `cluster` object.
`client_timeouts` is only at `nodes[].namespaces[].client_timeouts`.

```text
client_type, client_version, cluster_name, app_id
operational_metrics_enabled, usage_metrics_enabled
labels: map of application labels
latency_shift, latency_unit
recover_queue_size, invalid_node_count, delay_queue_timeout_count
total_nodes, open_connections
command_count, command_retries, batch_partial_failure
validation_open_failure, validation_tls_handshake_failure, validation_auth_failure
cpu_percent, memory_bytes
usage: map, keys listed in the usage table
cluster_latencies: [ { type, bucket_counts } ]
nodes: [
  {
    node_name, node_address, node_port
    connections: { in_use, in_pool, opened, closed }
    connection_open_failure, connection_tls_handshake_failure, connection_auth_failure
    conn_latency: { type, bucket_counts }    # type "conn"
    namespaces: [
      {
        namespace
        errors, server_timeouts, key_busy, record_too_big, device_overload
        bytes_in, bytes_out, client_timeouts
        latencies: [ { type, bucket_counts } ]
      }
    ]
  }
]
nodes_departed: same node shape
```

Node and namespace series include `nodes`, then any `nodes_departed` entry
whose `node_name` is not still in `nodes`. The same name in both lists is
published from `nodes` only. A node that leaves and rejoins before the
snapshot is built is only in `nodes`.

A namespace field is read only when that key is present on the namespace
object. A node connection field walks `connections`, then `in_use`,
`in_pool`, `opened`, or `closed`. A missing segment omits that point.

## Labels

Every series carries the identity that is present and non-empty:

| Snapshot field | Exported label |
|---|---|
| `cluster_name` | `cluster` |
| `client_type` | `client_type` |
| `client_version` | `client_version` |
| `app_id` | `app_id` |

Then the `labels` map on the document. A key in the table above is renamed
the same way. An exported name that is already set, or that is in the
reserved set below, is dropped. Exporter values win.

`client_app_ip` is on every series. The `labels` entry wins. When it is
absent, the fallback is the local address of the default route: a UDP socket
connects to `192.0.2.1` port `1` and `getsockname()` returns the address the
kernel would use. That connect sends no packet. A successful lookup is kept
for the process. A failed lookup omits the label and is tried again on the
next series. The result can differ from the service, pod, or node address.

Point labels are applied last. Node series add `server_node_id` from
`node_name`, `server_node_address` from `node_address`, and
`server_node_port` from `node_port`. An empty name or address is omitted.
A port is published when it is present, including `0`. Namespace series also
add `ns` from `namespace`.

Reserved names a webapp label must not replace:

`cluster`, `client_type`, `client_version`, `app_id`, `server_node_id`,
`server_node_address`, `server_node_port`, `ns`, `le`, `latency_type`,
`latency_unit`, `feature`, `conn_type`.

`feature` and `conn_type` are reserved. This catalog does not set them.

## Latency buckets

`cluster_latencies` feeds `aerospike.client.cluster.latency_bucket`.
`nodes[].namespaces[].latencies` feeds
`aerospike.client.namespace.latency_bucket`. `nodes[].conn_latency` feeds
`aerospike.client.node.conn_latency_bucket` when `type` is `conn`.

Cluster and namespace groups are `read`, `write`, `batch`, and `query`,
matched to the `type` field. The label `latency_type` is that group, or
`conn` for the node series. Each group is its own set of points. A group
with no `bucket_counts` array publishes nothing.

Buckets are already counted by the client. Publish them as cumulative
counters. `latency_shift` selects the width. The base is `2^latency_shift`
when that field is a number, and `2` when it is absent. The first finite
bound is `1`. Each later finite bound multiplies by the base. The last
bucket's `le` is `+Inf`. A non-numeric count is skipped and does not advance
the bound.

`latency_unit` on the document, when present and non-empty, is a label on
these points. Bounds are in that unit. With shift `3` and counts
`[1, 1, 1, 1]`, the `le` labels are `1`, `8`, `64`, `+Inf`. With the default
base and counts `[1, 2, 3]`, the cumulative values are `1`, `3`, `6` and the
`le` labels are `1`, `2`, `+Inf`.

## Catalog

Tier 0 is published whenever the document is present. Operational rows
require `operational_metrics_enabled: true`. Usage rows require
`usage_metrics_enabled: true`.

The unit is part of the instrument identity. The strings below are the ones
already registered. Changing a unit publishes a new series.

### Cluster

| Exported name | Kind | Tier | Unit | Snapshot path |
|---|---|---|---|---|
| `aerospike.client.cluster.recover_queue_size` | gauge | Tier 0 | `{connection}` | `recover_queue_size` |
| `aerospike.client.cluster.invalid_node_count` | counter | Tier 0 | `{event}` | `invalid_node_count` |
| `aerospike.client.cluster.delay_queue_timeout_count` | counter | Tier 0 | `{timeout}` | `delay_queue_timeout_count` |
| `aerospike.client.cluster.total_nodes` | gauge | Tier 0 | `{node}` | `total_nodes` |
| `aerospike.client.cluster.open_connections` | gauge | Tier 0 | `{connection}` | `open_connections` |
| `aerospike.client.cluster.validation_open_failure` | counter | Operational | `{error}` | `validation_open_failure` |
| `aerospike.client.cluster.validation_tls_handshake_failure` | counter | Operational | `{error}` | `validation_tls_handshake_failure` |
| `aerospike.client.cluster.validation_auth_failure` | counter | Operational | `{error}` | `validation_auth_failure` |
| `aerospike.client.cluster.retry_count` | counter | Operational | `{retry}` | `command_retries` |
| `aerospike.client.cluster.cpu_percent` | gauge | Operational | `%` | `cpu_percent` |
| `aerospike.client.cluster.memory_bytes` | gauge | Operational | `By` | `memory_bytes` |
| `aerospike.client.cluster.command_count` | counter | Operational | `{command}` | `command_count` |
| `aerospike.client.cluster.batch_partial_failure` | counter | Operational | `{command}` | `batch_partial_failure` |
| `aerospike.client.cluster.latency_bucket` | counter | Operational | `{operation}` | `cluster_latencies` |

`invalid_node_count` is the lifetime count of invalid-node events, not the
current node count. `total_nodes` is the current cluster size.

### Node

Labels: `server_node_id`, `server_node_address`, `server_node_port`. The
latency row also has `latency_type`, `le`, and `latency_unit`.

| Exported name | Kind | Tier | Unit | Snapshot path |
|---|---|---|---|---|
| `aerospike.client.node.connections_in_use` | gauge | Tier 0 | `{connection}` | `connections.in_use` |
| `aerospike.client.node.connections_in_pool` | gauge | Tier 0 | `{connection}` | `connections.in_pool` |
| `aerospike.client.node.connections_opened` | counter | Tier 0 | `{connection}` | `connections.opened` |
| `aerospike.client.node.connections_closed` | counter | Tier 0 | `{connection}` | `connections.closed` |
| `aerospike.client.node.connection_open_failure` | counter | Operational | `{error}` | `connection_open_failure` |
| `aerospike.client.node.connection_tls_handshake_failure` | counter | Operational | `{error}` | `connection_tls_handshake_failure` |
| `aerospike.client.node.connection_auth_failure` | counter | Operational | `{error}` | `connection_auth_failure` |
| `aerospike.client.node.conn_latency_bucket` | counter | Operational | `{operation}` | `conn_latency` |

The three `connection_*_failure` series are failures on a node that already
exists. The cluster `validation_*_failure` series are seed validation that
never becomes a node.

### Namespace

Labels: the node labels plus `ns`. The latency row also has `latency_type`,
`le`, and `latency_unit`.

| Exported name | Kind | Tier | Unit | Snapshot path |
|---|---|---|---|---|
| `aerospike.client.namespace.errors` | counter | Operational | `{error}` | `errors` |
| `aerospike.client.namespace.server_timeouts` | counter | Operational | `{timeout}` | `server_timeouts` |
| `aerospike.client.namespace.key_busy` | counter | Operational | `{error}` | `key_busy` |
| `aerospike.client.namespace.record_too_big` | counter | Operational | `{error}` | `record_too_big` |
| `aerospike.client.namespace.device_overload` | counter | Operational | `{error}` | `device_overload` |
| `aerospike.client.namespace.bytes_in` | counter | Operational | `By` | `bytes_in` |
| `aerospike.client.namespace.bytes_out` | counter | Operational | `By` | `bytes_out` |
| `aerospike.client.namespace.client_timeouts` | counter | Operational | `{timeout}` | `client_timeouts` |
| `aerospike.client.namespace.latency_bucket` | counter | Operational | `{operation}` | `latencies` |

### Usage

One counter per key in `usage`. Unit `{call}`. Published only when
`usage_metrics_enabled` is true and that key is present with a numeric value.

| Exported name | Snapshot key |
|---|---|
| `aerospike.client.feature.api_blocking` | `blocking` |
| `aerospike.client.feature.api_deferred` | `deferred` |
| `aerospike.client.feature.api_background` | `background` |
| `aerospike.client.feature.shape_point` | `shape_point` |
| `aerospike.client.feature.shape_batch` | `shape_batch` |
| `aerospike.client.feature.shape_query` | `shape_query` |
| `aerospike.client.feature.object_mapping_read` | `object_mapping_read` |
| `aerospike.client.feature.object_mapping_write` | `object_mapping_write` |
| `aerospike.client.feature.filter_ael` | `filter_ael` |
| `aerospike.client.feature.filter_exp` | `filter_exp` |

## Readers

Every reader on the provider collects the application's own instruments and
these client instruments. A periodic reader and a Prometheus reader each
report both families. A second periodic reader registered on that provider
collects the same series. The Prometheus scrape is that collection from the
provider. The application serves it with `exporter.start_http_server`, which
accepts TLS and mutual TLS and stops on `provider.shutdown()`, or with its
own `prometheus_client` server on a registry passed to the reader. That
webapp server keeps its TLS settings and keeps running after
`provider.shutdown()`. Details are in
[prometheus-reader.md](prometheus-reader.md).

A slower reader publishes only the newest document. A cumulative counter that
is still on the new document keeps its latest absolute total. A series that
existed only on a skipped document is not published.

Prometheus names in the Python OpenTelemetry exporter replace each dot with
an underscore and add `_total` on counter samples. Brace units such as
`{connection}` are not appended. Follow the OpenTelemetry Prometheus
compatibility rules in the target language.

## Out of scope

The metrics log file stays the client SDK writer (`exporter: file`,
`report_dir`). This exporter does not ship a writer.

Keeping every document until a reader has observed it, and changing a unit
string, are recorded as plans in [metrics.md](metrics.md). They are not part
of this translation.
