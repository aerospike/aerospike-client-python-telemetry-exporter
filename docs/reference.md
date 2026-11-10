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
`aerospike_client`. A counter row is an observable counter. A gauge row is an
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

This is the canonical document from the Python client SDK. Cluster fields
sit on `cluster`. A node is `name`, `address`, and `port`. A namespace is
`name`.

```text
client_type, client_version, cluster_name, app_id
operational_metrics_enabled, usage_metrics_enabled
labels: map of application labels
latency_shift, latency_unit
cluster:
  connections: { opened, closed, in_use, in_pool }
  nodes: { active, invalid }
  recover_queue: { size }
  command_timeout_client, command_retries
  cpu_percent, memory_bytes, command_count
  latency:
    read, write, batch, query:
      { buckets, sum, count, min, max }
usage: map, keys listed in the usage table
nodes: [
  {
    name, address, port
    connections:
      in_use, in_pool, opened, closed
      open_failure, tls_handshake_failure, auth_failure
      closed_error, closed_idle, closed_node_removed, pool_exhausted
    namespaces: [
      {
        name
        errors, timeouts, key_busy, record_too_big, device_overload
        bytes_in, bytes_out
        latency:
          conn, read, write, batch, query:
            { buckets, sum, count, min, max }
      }
    ]
  }
]
nodes_departed: same node shape
```

Node and namespace series include `nodes`, then any `nodes_departed` entry
whose `name` is not still in `nodes`. The same non-empty name in both lists
is published from `nodes` only. An empty name does not match another empty
name, so that departed node is still published from its address and port.
A node that leaves and rejoins before the snapshot is built is only in
`nodes`.

A namespace field is read only when that key is present on the namespace
object. A connection field walks `connections`. A missing segment omits that
point. `command_timeout_client` counts client-side expiries once for the
cluster. `timeouts` on a namespace counts timeouts the server reported. The
two do not overlap. Cluster connection totals, `command_timeout_client`,
`pool_exhausted`, and `error_rate` stay on the document and have no exported
series. `namespaces[].latency.conn` is pool acquisition. Socket-open latency
is `nodes[].latency.conn`, which stage does not write.

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

`client_app_ip` is not a series label. A pod address would create one series
per pod. `client_app_ip()` still returns the local address of the default
route: a UDP socket connects to `192.0.2.1` port `1` and `getsockname()`
returns the address the kernel would use. That connect sends no packet. A
successful lookup is kept for the process. A failed lookup returns an empty
string and is tried again on the next call. A `labels` entry named
`client_app_ip` is dropped and is not published.

Point labels are applied last. Node series add `node` from `name`,
`node_address` from `address`, and `node_port` from `port`. An empty name or
address is omitted. A port is published when it is present, including `0`.
Namespace series also add `namespace` from the namespace `name`.

Reserved names a webapp label must not replace:

`cluster`, `client_type`, `client_version`, `app_id`, `node`,
`node_address`, `node_port`, `namespace`, `le`, `latency_type`,
`latency_unit`, `feature`, `conn_type`.

`feature` and `conn_type` are reserved. This catalog does not set them.

## Latency buckets

`cluster.latency` feeds `aerospike_client_latency_bucket`.
`nodes[].namespaces[].latency` feeds
`aerospike_client_namespace_latency_bucket`.
`nodes[].latency.conn` feeds `aerospike_client_node_conn_latency_bucket`.

Cluster and namespace groups are `read`, `write`, `batch`, and `query`.
The namespace `conn` group is not exported. The node series uses
`latency_type=conn`. The label `latency_type` is the group name.
Each group is its own set of points. A group whose `count` is 0, or whose
`buckets` array is missing or empty, publishes nothing. A group with any
observation publishes every bucket, including a bucket whose count is 0.

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
| `aerospike_client_recover_queue_size` | gauge | Tier 0 | `{connection}` | `cluster.recover_queue.size` |
| `aerospike_client_invalid_node_count` | counter | Tier 0 | `{node}` | `cluster.nodes.invalid` |
| `aerospike_client_nodes_active` | gauge | Tier 0 | `{node}` | `cluster.nodes.active` |
| `aerospike_client_command_retries` | counter | Operational | `{retry}` | `cluster.command_retries` |
| `aerospike_client_cpu_percent` | gauge | Operational | `%` | `cluster.cpu_percent` |
| `aerospike_client_memory_bytes` | gauge | Operational | `By` | `cluster.memory_bytes` |
| `aerospike_client_command_count` | counter | Operational | `{command}` | `cluster.command_count` |
| `aerospike_client_latency_bucket` | counter | Operational | `{operation}` | `cluster.latency` |

`aerospike_client_nodes_active` is the current cluster size. The common name
table has no row for `cluster.nodes.active`; this name follows
`aerospike_client_invalid_node_count`. `invalid_node_count` is the client's
invalid-node count, not that size. Cluster connection totals are sums of the
node series and are not exported again. `command_timeout_client` has no name
in the table and is not exported.

### Node

Labels: `node`, `node_address`, `node_port`. The connection-latency row also
has `latency_type`, `le`, and `latency_unit`.

| Exported name | Kind | Tier | Unit | Snapshot path |
|---|---|---|---|---|
| `aerospike_client_node_connections_in_use` | gauge | Tier 0 | `{connection}` | `connections.in_use` |
| `aerospike_client_node_connections_in_pool` | gauge | Tier 0 | `{connection}` | `connections.in_pool` |
| `aerospike_client_node_connections_opened` | counter | Tier 0 | `{connection}` | `connections.opened` |
| `aerospike_client_node_connections_closed` | counter | Tier 0 | `{connection}` | `connections.closed` |
| `aerospike_client_node_connection_open_failure` | counter | Operational | `{error}` | `connections.open_failure` |
| `aerospike_client_node_connection_tls_handshake_failure` | counter | Operational | `{error}` | `connections.tls_handshake_failure` |
| `aerospike_client_node_connection_auth_failure` | counter | Operational | `{error}` | `connections.auth_failure` |
| `aerospike_client_node_connection_closed_error` | counter | Operational | `{connection}` | `connections.closed_error` |
| `aerospike_client_node_connection_closed_idle` | counter | Operational | `{connection}` | `connections.closed_idle` |
| `aerospike_client_node_connection_closed_node_removed` | counter | Operational | `{connection}` | `connections.closed_node_removed` |
| `aerospike_client_node_conn_latency_bucket` | counter | Operational | `{operation}` | `latency.conn` |

The three `connection_*_failure` series are failures on a node that already
exists. Close-reason counters are the causes stage classifies. Stage has no
`closed_pool_shrink`. `aerospike_client_node_conn_latency_bucket` is a
successful socket open. Stage does not write `nodes[].latency.conn`, so that
series is empty on a stage snapshot.

### Namespace

Labels: the node labels plus `namespace`. The latency row also has
`latency_type`, `le`, and `latency_unit`.

| Exported name | Kind | Tier | Unit | Snapshot path |
|---|---|---|---|---|
| `aerospike_client_namespace_errors` | counter | Operational | `{error}` | `errors` |
| `aerospike_client_namespace_server_timeouts` | counter | Operational | `{timeout}` | `timeouts` |
| `aerospike_client_namespace_key_busy` | counter | Operational | `{error}` | `key_busy` |
| `aerospike_client_namespace_record_too_big` | counter | Operational | `{error}` | `record_too_big` |
| `aerospike_client_namespace_device_overload` | counter | Operational | `{error}` | `device_overload` |
| `aerospike_client_namespace_bytes_in` | counter | Operational | `By` | `bytes_in` |
| `aerospike_client_namespace_bytes_out` | counter | Operational | `By` | `bytes_out` |
| `aerospike_client_namespace_latency_bucket` | counter | Operational | `{operation}` | `latency` |

Namespace latency groups are `read`, `write`, `batch`, and `query`. The
`conn` group on that object is pool acquisition and is not exported.

### Usage

One counter per v1 key in `usage`. Unit `{call}`. Published only when
`usage_metrics_enabled` is true and that key is present with a numeric value.
Later `feature.*` rows stay off this list until they are promoted.

| Exported name | Snapshot key |
|---|---|
| `aerospike_client_feature_api_blocking` | `feature.api.blocking` |
| `aerospike_client_feature_api_deferred` | `feature.api.deferred` |
| `aerospike_client_feature_api_background` | `feature.api.background` |
| `aerospike_client_feature_shape_point` | `feature.shape.point` |
| `aerospike_client_feature_shape_batch` | `feature.shape.batch` |
| `aerospike_client_feature_shape_query` | `feature.shape.query` |
| `aerospike_client_feature_filter_ael` | `feature.filter.ael` |
| `aerospike_client_feature_filter_exp` | `feature.filter.exp` |

A key the snapshot does not contain is omitted. The Python SDK has no object
mapping, so `aerospike_client_feature_object_mapping_read` and
`aerospike_client_feature_object_mapping_write` are not registered.

## Readers

Every reader on the provider collects the application's own instruments and
these client instruments. A periodic reader and a Prometheus reader each
report both families. A second periodic reader registered on that provider
collects the same series. Two exporters constructed with the same provider
share one registry and one set of instruments, so both clients appear on
those readers. The Prometheus scrape is that collection from the provider.
The Python reader uses the same conversion settings as
`PrometheusMetricReader` 0.66b1. Temporality is cumulative for every
instrument class and is not configurable. The application serves the scrape
with `exporter.start_http_server`, which accepts TLS and mutual TLS and stops
on `provider.shutdown()`, or with its own `prometheus_client` server on a
registry passed to the reader. That webapp server keeps its TLS settings and
keeps running after `provider.shutdown()`. Shutdown does not wait for a
scrape callback and does not cancel it. The server certificate and key must
match, and client CA material must contain CA certificates only. Constructor
fields, including the resource-attribute filter and the private registry
default, are in [prometheus-reader.md](prometheus-reader.md).

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
