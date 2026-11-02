# Aerospike Python client telemetry exporter

External exporters for Aerospike Python client metrics. The client SDK collects
the snapshot and calls `export()`. This package translates that snapshot. It
does not open an OTLP connection and it does not choose HTTP or gRPC.

Series names, labels, tiers, and how a snapshot becomes those series are in
[docs/metrics.md](docs/metrics.md). The field-by-field catalog for a Go or
other client exporter is in [docs/reference.md](docs/reference.md).

## Contract

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

The webapp records its own instruments on this same provider. Every reader
on the provider collects those instruments and the client series. A periodic
reader and a Prometheus reader each report both families. A second periodic
reader, registered later with `add_metric_reader`, collects the same series.
The Prometheus scrape is that collection from the provider.

The metrics log file stays the client SDK `MetricsWriter`, with `exporter: file`
and `report_dir`. This package does not ship a writer.

## Dependencies

Python 3.11 or newer. The package version is the root `VERSION` file.

| Package | When |
|---|---|
| `aerospike-sdk>=1.0.0` | Always. The client that collects the snapshot and calls `export()`. |
| `opentelemetry-api>=1.27.0` | `[otel]` extra. |
| `opentelemetry-sdk>=1.27.0` | `[otel]` extra. |
| `opentelemetry-exporter-otlp-proto-grpc>=1.27.0` | `[otel]` extra. |
| `opentelemetry-exporter-otlp-proto-http>=1.27.0` | `[otel]` extra. |
| `pytest>=8.0.0`, `pytest-asyncio>=0.21.0`, `testcontainers>=4.0` | `[dev]` extra. |

A higher version that is already installed stays. `[otel]` is only needed when
those libraries are not already in the environment. Prometheus scrape tests
install `opentelemetry-exporter-prometheus` separately. It is not a dependency
of this package.

```bash
pip install aerospike-client-telemetry-exporter
pip install 'aerospike-client-telemetry-exporter[otel]'
```

## Project

```text
.
├── aerospike_client_telemetry/
│   ├── common/          # metric catalog and snapshot registry
│   ├── recording/       # in-memory exporter for tests
│   └── otel/            # OpenTelemetry exporter; optional import
├── docs/
│   ├── metrics.md       # series this package publishes
│   └── reference.md     # catalog and snapshot shape for another language
├── tests/
│   ├── unit/
│   └── integration/     # Aerospike, collector, and optional cloud
├── pyproject.toml
└── VERSION
```

setuptools builds the package and reads the version from `VERSION`. The
`[otel]` extra is optional so the root import stays free of OpenTelemetry.

## Compilation and install

The package is pure Python. A local checkout compiles and installs like this:

```bash
git clone https://github.com/aerospike/aerospike-client-python-telemetry-exporter.git
cd aerospike-client-python-telemetry-exporter
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ../aerospike-client-python-sdk
python -m pip install -e '.[dev,otel]'
python -m compileall -q aerospike_client_telemetry tests
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
python -m pytest tests/integration/otel/test_cloud.py::test_webapp_sends_to_an_otlp_cloud_when_configured -v
```

One case runs per provider and protocol. A missing URL or required header
skips that case. The HTTP value is the metrics URL the exporter POSTs to. A
gRPC value without a scheme still uses TLS. Every case uses service name
`python-orders-service`. The `app_id` is `orders-<provider>-http` or
`orders-<provider>-grpc`.

| Provider | HTTP variable | Header variable |
|---|---|---|
| New Relic | `OTEL_EXPORTER_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_HEADERS` (`api-key=<license>`) |
| Datadog | `OTEL_EXPORTER_OTLP_ENDPOINT_DATADOG` | `OTEL_EXPORTER_OTLP_HEADERS_DATADOG` (`dd-api-key=<key>`) |
| Dynatrace | `OTEL_EXPORTER_OTLP_ENDPOINT_DYNATRACE` | `OTEL_EXPORTER_OTLP_HEADERS_DYNATRACE` (`Authorization=Bearer <token>`) |
| Amazon CloudWatch | `OTEL_EXPORTER_OTLP_ENDPOINT_CLOUDWATCH` | `OTEL_EXPORTER_OTLP_HEADERS_CLOUDWATCH` (`Authorization=Bearer <token>`) |
| Google Cloud Monitoring | `OTEL_EXPORTER_OTLP_ENDPOINT_GOOGLE_CLOUD` | `OTEL_EXPORTER_OTLP_HEADERS_GOOGLE_CLOUD` (`Authorization=Bearer <token>`) |
| Grafana Cloud | `OTEL_EXPORTER_OTLP_ENDPOINT_GRAFANA` | `OTEL_EXPORTER_OTLP_HEADERS_GRAFANA` (`Authorization=Basic <token>`) |
| Prometheus OTLP receiver | `OTEL_EXPORTER_OTLP_ENDPOINT_PROMETHEUS` | `OTEL_EXPORTER_OTLP_HEADERS_PROMETHEUS`, optional |
| Sumo Logic | `OTEL_EXPORTER_OTLP_ENDPOINT_SUMOLOGIC` | `OTEL_EXPORTER_OTLP_HEADERS_SUMOLOGIC`, optional (`x-sumo-token=<token>`) |
| OpenObserve | `OTEL_EXPORTER_OTLP_ENDPOINT_OPENOBSERVE` | `OTEL_EXPORTER_OTLP_HEADERS_OPENOBSERVE` (`Authorization=Basic <token>`) |

New Relic, Dynatrace, Google Cloud Monitoring, Grafana Cloud, and OpenObserve
also read `OTEL_EXPORTER_OTLP_GRPC_ENDPOINT` with the same suffix as the HTTP
variable. New Relic's gRPC variable has no suffix. Datadog, CloudWatch,
Prometheus, and Sumo Logic are HTTP only here. CloudWatch normally
authenticates with SigV4. This case does not sign a request. It sends the
bearer token when that header is set. Pull-request integration passes the
same names from Actions secrets. An unset secret skips that provider.

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
