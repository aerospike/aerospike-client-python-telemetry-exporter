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
The OpenTelemetry reader sends OTLP later. A failed send does not raise
from `export`, so the client keeps calling it. `timeout` bounds the
OpenTelemetry client's 6 retries. There is no retry-count setting.
`close()` is what makes a later `export` raise. Install that backend
with `pip install -e ".[otel]"`.
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
The image's single-node evaluation feature key is used. The container
publishes that host port as its access address, so the client connects
directly. Set `AEROSPIKE_HOST` to use a server that is already running
instead. If that server advertises an address the client cannot reach, set
`AEROSPIKE_USE_SERVICES_ALTERNATE=true`. Tests use the in-memory namespace
`test`. The server also has `persist`, stored in a file. Tests
register the exporter in `CUSTOM` mode and wait for the real scheduler. They
do not call private timer methods.

The same list is the test catalog in `SHARED_DESIGN.md` under
`~/Downloads/om372_client_lib_python`.

### Unit

`tests/unit/recording/test_exporter.py` — sync and async recorder shape,
translation without mutation, no snapshot retention, injected failure.

`tests/unit/otel/test_package.py` — importing OTel does not import
`opentelemetry`, the root package does not export `OtelExporter`, config
copies headers and leaves temporality unset, sync publish then `close`
rejects another export, async publish, catalog conflict.

`tests/unit/otel/test_multiple_exporters.py` — two exporter objects keep two
registries; two publishes on one exporter keep the latest document.

`tests/unit/common/test_catalog.py` — one label rename for every metric,
`client_app_ip` on every series,
reserved webapp labels dropped, empty node or namespace names omitted,
duplicate node names both yielded, departed nodes and non-numeric values
omitted, unmapped fields absent, and the registry keeps the latest document.

`tests/unit/otel/test_provider.py` — `OtelExporter()` builds a provider from
`OTEL_*` and `close()` shuts it down; an endpoint and service name override
that environment; a passed-in meter provider is enough and is not shut down;
a provider plus an endpoint does not open a second OTLP connection; unset
temporality follows the environment and an explicit value overrides it;
`proxy_url` is set on the HTTP session and rejected for gRPC. HTTP appends
`/v1/metrics`; gRPC keeps the endpoint without that path. A stalled
endpoint times out, and a configured proxy with nothing listening fails
the send. Both are logged, and the next `export` still stores the document.

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
in. The exporter does not open OTLP. Enable and disable start and stop the
Aerospike export thread, and the provider keeps reporting the last
`command_count` while that thread is stopped.

`tests/integration/otel/test_webapp.py` — customer startup, an order write,
operational and usage metrics, `client_app_ip` on the snapshot, and
shutdown. A second case registers that one exporter on two clients,
`telemetry-it` and `telemetry-it-two`, and each cluster keeps its own
document.

`tests/integration/otel/test_collector.py` — the same webapp shape, with
operational and usage metrics on, once over HTTP/protobuf and once over
gRPC. It inserts, upserts, and then reads by point query, bin projection,
exists, header, batch, set scan, and filtered query. Snapshot latency
totals match those calls: read, write, batch, and query. Prometheus
`command_count` matches the calls except single-key `exists`, which the
SDK fast path does not count. The five latency bucket names still do not
arrive as their own series. With `-s`, the test prints the comparison.

`tests/integration/otel/test_identity_defaults.py` — `app_id` is the name
that keeps a client unique on one cluster. Omitting it stores `not-set`,
unless the client has a username, which is then used as `app_id`.
Omitting `cluster_name` stores the server name. The SDK fills client type
and version. Omitted labels are empty. Two clients on the default app id
share one snapshot. Two app ids keep both. The same app id on two clusters
stays two series, because `cluster` is on every series.

`tests/integration/otel/test_multiple_exporters.py` — two OTel exporters each
receive the snapshot, three exporters receive the same metrics aside from
`timestamp`, registering the same exporter twice calls `export` twice while
the registry keeps one document, and two clusters that each register a
different exporter keep separate registries.

## License

Apache License 2.0. See [LICENSE](LICENSE).
