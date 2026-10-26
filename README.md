# Aerospike Client Telemetry Exporter

External exporter for the Aerospike Python client. The client SDK collects
metrics and pushes a `MetricsSnapshot` on its periodic scheduler. This package
translates that snapshot. It does not collect metrics and it does not own the
client lifecycle.

```text
aerospike_client_telemetry/
├── common/       shared catalog and per-client registry
├── recording/    in-memory exporter for the periodic callback
└── otel/         OpenTelemetry exporter; depends on the otel extra
tests/
├── unit/recording
├── unit/otel
├── integration/recording/{sync,async}
├── integration/otel
└── integration/docker          Aerospike server, collector, and Prometheus configs.
                                Those containers share the Compose project aerospike-python-telemetry
```

`common` has no vendor dependency. `recording` proves the client scheduler.
`otel` publishes each canonical document into the registry and returns.
The accepted contract is that the webapp passes a `MeterProvider` and this
package only registers instruments on it. It does not create a resource, an
OTLP exporter, a periodic reader, or a meter provider. The reader already on
that provider sends OTLP later. A failed send does not raise from `export`,
so the client keeps calling it. `close()` does not shut the provider down;
it is what makes a later `export` raise. The earlier implementation that
built the pipeline is preserved on branch `OM-373-DONT-DELETE`. Install the
backend with `pip install -e ".[otel]"`.
Import it from `aerospike_client_telemetry.otel`. The top-level package does not
load it.

## Setup

```bash
/opt/homebrew/bin/python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e /Users/pmokrala/aerospike_source/aerospike-client-python-sdk
python -m pip install -e ".[dev]"
python -m pip show aerospike-sdk aerospike-native
```

`aerospike-sdk` must report the local SDK checkout as its editable project
location. The client branch used here is `CLIENT-OM-372`.

## Snapshot contract

The exporter consumes the revised `MetricsSnapshot.to_canonical_dict()` shape:

- cluster totals and whole-call read/write/batch/query latency are top-level;
- each node has one four-field pool, connection latency, and existing-node
  open/TLS/auth failure counters;
- each namespace has the error split, bytes, and RPC read/write/batch/query
  latency summaries;
- usage has only the approved short names;
- `conn_type`, close reasons, pool exhaustion, and node error rate are absent.

`operational_metrics_enabled is False` omits the operational series, and
`usage_metrics_enabled is False` omits feature usage. A missing flag does not
hide a series. Always-on gauges are published whenever a document is stored.
`nodes_departed` stays on the document and is not published.

Every series carries `client_type`, `client_version`, `cluster`, and `app_id`, plus `client_app_ip` when it can be detected. Customer labels from `MetricsPolicy.labels` are added unless the name collides with one of those. Node and namespace series add `server_node_id` (the server node id), `server_node_address`, `server_node_port`, and `ns`. Latency buckets add `le`. Feature usage adds `feature`.

The SDK/native layer does not yet provide delay-queue timeout count, pre-node
validation failures, namespace-attributed client timeouts, or authoritative
batch-partial failures. The exporter does not invent zeroes for them.

Async SDK clients preserve the application's event loop by default. For
custom exporters that may block, the client can opt into a dedicated exporter
event loop with `MetricsSettings(async_exporter_isolation=True)`. An exporter
using that mode must not depend on asyncio objects bound to the application
loop.

## Tests

```bash
.venv/bin/python -m pytest tests/unit
.venv/bin/python -m pytest tests/integration
.venv/bin/python -m pytest tests/integration/otel -s
.venv/bin/python -m pytest tests/integration/recording -s
```

`-q` prints one dot per test. `-s` shows `print` output. Unit tests do not
need a server. Integration tests start one Aerospike Enterprise `8.2.0.0`
container for the session, on an ephemeral host port, and stop it afterward.
The image's single-node evaluation feature key is used, so that container
cannot form a two-node cluster. The one-node-down test starts its own
Community Edition `8.2.0.0` pair for that reason. The container
publishes that host port as its access address, so the client connects
directly. Set `AEROSPIKE_HOST` to use a server that is already running
instead. If that server advertises an address the client cannot reach, set
`AEROSPIKE_USE_SERVICES_ALTERNATE=true`. Tests use the in-memory namespace
`test`. The server also has `persist`, stored in a file. Tests
register the exporter in `CUSTOM` mode and wait for the real scheduler. They
do not call private timer methods.

Pull requests to `dev` and `main` run `.github/workflows/build-test.yml`.
That workflow lints with Ruff, compiles on Python 3.11, runs the unit tests
on 3.11 and 3.14, and runs the integration tests on 3.14. It installs the
client SDK from commit `3463312` on `CLIENT-OM-372` and leaves
`AEROSPIKE_HOST` unset, so the integration suite starts its own containers.
Each test job uploads `junit.xml` and `pytest.log` to the Actions run for
90 days. A failed integration run also uploads the container logs. The
workflow does not publish a wheel.

The same list is the test catalog in `SHARED_DESIGN.md` under
`~/Downloads/om372_client_lib_python`. Open work is
`~/Downloads/om372_client_lib_python/BACKLOG.md`.

### Unit

`tests/unit/recording/test_exporter.py` — sync and async recorder shape,
translation without mutation, no snapshot retention, injected failure.

`tests/unit/otel/test_package.py` — importing OTel does not import
`opentelemetry`, the root package does not export `OtelExporter`, sync
publish then `close` rejects another export, async publish, catalog conflict.

`tests/unit/otel/test_multiple_exporters.py` — two exporter objects keep two
registries; two publishes on one exporter keep the latest document.

`tests/unit/common/test_catalog.py` — one label rename for every metric,
`client_app_ip` on every series,
reserved webapp labels dropped, empty node or namespace names omitted,
duplicate node names both yielded, departed nodes and non-numeric values
omitted, revised latency scopes, existing-node connection failures, unmapped
fields absent, a disabled operational or usage tier omits that tier and keeps
the always-on gauges, and the registry keeps the latest document.

`tests/unit/otel/test_provider.py` — a meter provider is required. The
exporter registers the catalog on that provider and `close()` does not shut
it down.

`tests/unit/otel/test_reader_interval.py` — a real periodic reader on a
different clock from `export()`. A reader faster than the client push
reports the same absolute command count and node count again, and does not
double them. A reader slower than the client push publishes only the latest
snapshot; the overwritten command count and node count are absent. The waits
are shortened. The ratios match a 15-second reader against a 30-second push,
and a 60-second reader against a 30-second push.

### Integration

`tests/integration/recording/sync/test_sync_periodic_export.py` and
`tests/integration/recording/async/test_async_periodic_export.py` — one
callback, two exporters on the same cycle, a failure does not stop the other
exporter, disable stops callbacks without closing the exporter, re-enable
resumes. The sync file also turns metrics on and off four times: each
disable joins the export thread without an error, and the next enable
starts a new one.

`tests/integration/recording/sync/test_sync_json_export.py` — the recording
exporter and a JSON writer receive the same cycle. The file is
`build/recording-snapshot.json`.

`tests/integration/otel/test_sdk_install.py` — public SDK version check, then
one exporter registration.

`tests/integration/otel/test_passed_provider.py` — a meter provider passed
in. The exporter does not open OTLP. The webapp's own counter and gauge are
collected from that same provider beside `aerospike_client_command_count`.
Enable and disable start and stop the Aerospike export thread. The provider
keeps reporting the last client `command_count` while that thread is stopped,
and the webapp instruments keep moving. With metrics still enabled, turning
operational metrics off and back on drops and restores command count while
node count stays; turning feature usage off and back on does the same for
`aerospike_client_feature_usage`.

`tests/integration/otel/test_server_down.py` — metrics stay enabled. When the
only server stops, the client keeps that seed while tend is refused, so the
next snapshots still list it: node count 1, invalid count 0, empty
`nodes_departed`, and the last command count. The callback keeps publishing
that picture. When one node of a two-node cluster stops, that host appears
once in `nodes_departed` and is not published. Node count becomes 1, and the
callback keeps connection series for the survivor only.

`tests/integration/otel/test_webapp.py` — customer startup, an order write,
operational and usage metrics, `client_app_ip` on the snapshot, and
shutdown. A second case registers that one exporter on two clients,
`telemetry-it` and `telemetry-it-two`, and each cluster keeps its own
document.

`tests/integration/otel/test_collector.py` — the test builds the meter
provider and its OTLP reader, once over HTTP/protobuf and once over gRPC,
with operational and usage metrics on. It inserts, upserts, and then reads
by point query, bin projection,
exists, header, batch, set scan, and filtered query. Snapshot latency
totals match those calls: read, write, batch, and query. Prometheus
`command_count` matches the calls except single-key `exists`, which the
SDK fast path does not count. Whole-call cluster read/write/batch/query,
node connection-acquisition, and namespace RPC latency buckets arrive as
cumulative `_bucket` series with `le`; each `+Inf` bucket equals its
corresponding snapshot total. With `-s`, the test prints the comparison.

`tests/integration/otel/test_identity_defaults.py` — `app_id` is the name
that keeps a client unique on one cluster. Omitting it stores `not-set`,
unless the client has a username, which is then used as `app_id`.
Omitting `cluster_name` stores the server name. The SDK fills client type
and version; connection mode is not part of the revised snapshot identity.
Omitted labels are empty. Two clients on the default app id
share one snapshot. Two app ids keep both. The same app id on two clusters
stays two series, because `cluster` is on every series.

`tests/integration/otel/test_multiple_exporters.py` — two OTel exporters each
receive the snapshot, three exporters receive the same canonical document
including its stable timestamp, registering the same exporter twice calls `export` twice while
the registry keeps one document, and two clusters that each register a
different exporter keep separate registries.

## License

Apache License 2.0. See [LICENSE](LICENSE).
