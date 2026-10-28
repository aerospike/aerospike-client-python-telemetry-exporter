# Aerospike Python client telemetry exporter

External exporters for Aerospike Python client metrics. The client SDK collects
the snapshot and calls `export()`. This package translates that snapshot. It
does not open an OTLP connection and it does not choose HTTP or gRPC.

The webapp builds the OpenTelemetry `MeterProvider`, including the reader and
the OTLP exporter, and passes that provider in. HTTP and gRPC are whatever
client the webapp attached. A cloud such as New Relic is that provider's
endpoint and header (`api-key` for New Relic). This package never sees the key.

```python
from opentelemetry.sdk.metrics import MeterProvider

from aerospike_client_telemetry.otel import OtelExporter

exporter = OtelExporter(provider)
```

`AsyncOtelExporter` is the same contract for the async client. The client
runs that call on its metrics-owned event loop. `export()` only stores the
snapshot, so it does not use the application's event loop. The SDK no longer
has `async_exporter_isolation`; async delivery is always isolated. Both
exporters live in `aerospike_client_telemetry.otel`. Importing the root
package does not load OpenTelemetry.

Register the exporter in `CUSTOM` mode with `add_exporter`, then
`enable_metrics`. On shutdown, disable metrics, close the cluster, and call
`provider.shutdown()`. The exporter has no `close()`. The webapp owns the
provider, and shutting it down drops the observable callbacks.

`export()` stores the latest snapshot and returns. A failed OTLP send does not
raise from `export()`. If a newer `aerospike-sdk` or OpenTelemetry release
changes a call this package makes, `export()` lets that exception propagate.
The client logs it on the metrics thread, keeps the webapp serving, and
suspends this exporter after three consecutive failures. A failure while
constructing `OtelExporter` happens on the startup thread, before registration,
so startup fails.

Install floors are `aerospike-sdk>=1.0.0` and, for the OpenTelemetry extra,
`opentelemetry-*>=1.27.0`. A higher version that is already installed stays.
`[otel]` is only needed when those libraries are not already in the environment.

```bash
pip install aerospike-client-telemetry-exporter
pip install 'aerospike-client-telemetry-exporter[otel]'
```

Python 3.11 or newer.

## What is exported

The snapshot is one document per client. The identity is `client_type`,
`client_version`, `cluster_name`, and `app_id`. Two clients that share those
four values are one series. The webapp sets a distinct `app_id` per client.
There is no `cluster` object in the document. Cluster-wide fields sit on the
document next to `command_count`.

`client_timeouts` is only on each namespace object,
`nodes[].namespaces[].client_timeouts`. It is not repeated on the document.

Tier 0 is published whenever metrics are enabled. Operational series are
omitted when `operational_metrics_enabled` is false. Usage series are omitted
when `usage_metrics_enabled` is false. A missing flag does not hide a series.

Every metric is `aerospike.client.<context>.<stat>`. Cluster-wide series use
`cluster`, for example `aerospike.client.cluster.command_count`. Node series
use `node`, for example `aerospike.client.node.connections_in_use`. Namespace
series use `namespace`. Feature counters use `feature`. A Prometheus exporter
replaces each dot with an underscore, so the same series is
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
| `aerospike.client.cluster.latency_bucket` | counter | Operational | Cluster whole-call latency. Label `operation` is `read`, `write`, `batch`, or `query`. |
| `aerospike.client.namespace.latency_bucket` | counter | Operational | Namespace RPC latency. Same `operation` label, plus the node labels and `ns`. |
| `aerospike.client.node.conn_latency_bucket` | counter | Operational | Connection-acquisition latency on a node. |
| `aerospike.client.namespace.client_timeouts` | counter | Operational | Client-side timeouts on that namespace. Stays 0 until native reports it. |
| `aerospike.client.feature.api_blocking` | counter | Usage | One counter per feature. The other usage names follow the same pattern: `aerospike.client.feature.api_deferred`, `aerospike.client.feature.api_background`, `aerospike.client.feature.shape_point`, `aerospike.client.feature.shape_batch`, `aerospike.client.feature.shape_query`, `aerospike.client.feature.object_mapping_read`, `aerospike.client.feature.object_mapping_write`, `aerospike.client.feature.filter_ael`, `aerospike.client.feature.filter_exp`. |

Latency buckets are cumulative counters with an `le` label. The snapshot field
`latency_unit` is a label on those three latency metrics. An existing-node
open, TLS, or auth failure stays on the node
(`aerospike.client.node.connection_open_failure` and the TLS and auth names).
That is a different series from the cluster validation counters above.

Labels on every series include `cluster`, `client_type`, `client_version`,
`app_id`, and `client_app_ip`. Node series add `server_node_id`,
`server_node_address`, and `server_node_port`. Namespace series also add `ns`.
A webapp label that uses one of those names is dropped. Exporter values win.

The metrics log file is the client SDK `MetricsWriter`. A later SDK field,
`report_format`, defaults to `log`, the positional file written today.
`jsonl` is opt-in. This package does not ship a second writer.

## Project structure

```text
.
├── aerospike_client_telemetry/
│   ├── common/          # metric catalog and snapshot registry
│   ├── recording/       # in-memory exporter for tests
│   └── otel/            # OpenTelemetry exporter; optional import
├── tests/
│   ├── unit/
│   └── integration/     # Aerospike, collector, and optional cloud
├── pyproject.toml
└── VERSION
```

## Development setup

```bash
git clone https://github.com/aerospike/aerospike-client-python-telemetry-exporter.git
cd aerospike-client-python-telemetry-exporter
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ../aerospike-client-python-sdk
python -m pip install -e '.[dev,otel]'
```

`../aerospike-client-python-sdk` is a checkout of the Python client. Use a
released `aerospike-sdk` when that release contains the snapshot this package
reads.

```bash
python -m pytest tests/unit
python -m pytest tests/integration
```

Integration tests start their own Aerospike containers when `AEROSPIKE_HOST`
is unset. One webapp case sends a snapshot to a real OTLP endpoint. It runs
only when the variables for that protocol are set. Otherwise that case is
skipped.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.nr-data.net/v1/metrics
export OTEL_EXPORTER_OTLP_GRPC_ENDPOINT=otlp.nr-data.net:4317
export OTEL_EXPORTER_OTLP_HEADERS=api-key=$NEW_RELIC_LICENSE_KEY
python -m pytest tests/integration/otel/test_webapp.py::test_webapp_sends_to_an_otlp_cloud_when_configured -v
```

The HTTP URL includes `/v1/metrics`. The gRPC value is host and port `4317`.
A missing scheme still uses TLS. Both cases use service name
`python-orders-service`. HTTP points have `app_id` `orders-http`. gRPC points
have `app_id` `orders-grpc`. Pull-request integration runs the same case when
Actions secrets of those three names are set.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute to this repository.

## Security

For information on reporting security vulnerabilities, please see [SECURITY.md](SECURITY.md).

## Repo tooling

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on repo tooling and development environment setup.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For questions or issues, please:

- Open an issue on GitHub
- Check existing documentation
- Contact the maintainers
