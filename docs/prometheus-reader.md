# Prometheus scrape reader

Status: implemented.

`CustomPrometheusMetricReader` converts OpenTelemetry metrics into a
Prometheus registry. Either the exporter or the webapp starts the listener.
The beta
package `opentelemetry-exporter-prometheus` 0.66b1 is a development-time
comparison target only. It is not a production dependency. The tested pair is
OpenTelemetry SDK 1.45.1 and `prometheus-client` 0.26.0.

## Boundary

The reader extends `MetricReader`. It does not extend the beta reader, and it
does not register instruments or accept an HTTP server. `OtelExporter` still
publishes snapshots onto the meter provider the application supplies.
`start_http_server` is the additional method that opens the scrape port.

When the exporter owns the listener, it validates the bind address and TLS
material, then calls `prometheus_client.start_http_server` on the reader's
registry. The application calls `exporter.start_http_server` after the
provider exists. A webapp that owns the listener starts that server itself.
This package does not install SIGTERM or SIGINT handlers. The host process
calls `provider.shutdown()` on graceful shutdown. That calls the reader, which
unregisters its collector and stops the server the exporter started.
`MeterProvider(shutdown_on_exit=True)` is only the interpreter-exit fallback.
SIGKILL does not run that cleanup.

Each process binds its own port. This reader does not combine metrics across
worker processes. A port already in use raises `OSError` and does not leave a
thread behind.

## Install

```bash
pip install 'aerospike-client-telemetry-exporter[otel,prometheus]'
```

`prometheus-client` is the `[prometheus]` extra. Importing
`aerospike_client_telemetry.otel` does not import it. The reader module does.

The exporter-owned server does not require application code to import
`prometheus_client`. A webapp that already serves Prometheus imports
`prometheus_client` itself and passes its registry to the reader.

Certificate and key files belong to the application. This package does not
ship them. Tests mint them with `openssl` in a temporary directory, and
`.gitignore` ignores `*.pem`, `*.crt`, `*.csr`, `*.key`, and `*.srl`.

## Use cases

### Exporter-owned scrape server

Construct `CustomPrometheusMetricReader` with conversion settings only. The
exporter opens the port. `provider.shutdown()` stops that server.

```python
from aerospike_client_telemetry.otel import OtelExporter
from aerospike_client_telemetry.otel.prometheus_reader import (
    CustomPrometheusMetricReader,
)

prom_reader = CustomPrometheusMetricReader()
provider = MeterProvider(metric_readers=[periodic_reader, prom_reader])
exporter = OtelExporter(provider)
exporter.start_http_server(host="127.0.0.1", port=9464)
provider.shutdown()
```

The default bind address is `127.0.0.1:9464`. `PrometheusConfig` groups the
host, port, and TLS config. Pass `config=` or `host`, `port`, and
`tls_config`, one form at a time.

TLS uses a server certificate and key. A scrape client does not present a
certificate:

```python
from aerospike_client_telemetry.otel.prometheus_reader import TLSConfig

exporter.start_http_server(
    host="127.0.0.1",
    port=9464,
    tls_config=TLSConfig(certfile="server.crt", keyfile="server.key"),
)
```

Mutual TLS also requires a client certificate signed by the configured CA:

```python
exporter.start_http_server(
    host="127.0.0.1",
    port=9464,
    tls_config=TLSConfig(
        certfile="server.crt",
        keyfile="server.key",
        client_cafile="ca.crt",
        client_auth_required=True,
    ),
)
```

`TLSConfig` fields are `certfile`, `keyfile`, `client_cafile`,
`client_capath`, `client_auth_required`, `tls_min_version`, and
`tls_max_version`.

### Webapp-owned scrape server

The webapp keeps its `CollectorRegistry` and calls
`prometheus_client.start_http_server`. Pass that registry to the reader. Do
not call `exporter.start_http_server`. A scrape collects the client
instruments into the webapp registry. `provider.shutdown()` unregisters the
reader and leaves the webapp server running.

The same TLS and mutual TLS fields are arguments of
`prometheus_client.start_http_server`: `certfile`, `keyfile`,
`client_cafile`, `client_capath`, `client_auth_required`, `tls_min_version`,
and `tls_max_version`.

```python
from prometheus_client import CollectorRegistry, start_http_server

from aerospike_client_telemetry.otel import OtelExporter
from aerospike_client_telemetry.otel.prometheus_reader import (
    CustomPrometheusMetricReader,
)

registry = CollectorRegistry()
prom_reader = CustomPrometheusMetricReader(registry=registry)
provider = MeterProvider(metric_readers=[prom_reader])
exporter = OtelExporter(provider)
start_http_server(
    9464,
    addr="127.0.0.1",
    registry=registry,
    certfile="server.crt",
    keyfile="server.key",
)
provider.shutdown()
```

Mutual TLS adds `client_cafile="ca.crt"` and `client_auth_required=True` to
that `start_http_server` call. Omit `certfile` and `keyfile` for plaintext.

### Webapp metrics and client metrics on one scrape

Prometheus metrics the webapp registers on its own registry stay there.
OpenTelemetry instruments the webapp creates on the same provider are
collected by the reader into that registry, beside the client series. One
scrape returns all three. After `provider.shutdown()` the webapp's native
series remain and the reader series are gone. The webapp server, including
its TLS or mutual TLS configuration, keeps running.

## Collection

A scrape calls `MetricReader.collect()`. The SDK runs the callbacks already
registered on the provider and passes one `MetricsData` batch to
`_receive_metrics`. A periodic OTLP reader collects independently and can
observe a different snapshot generation.

Counters request cumulative temporality. Monotonic sums become Prometheus
counters. Cumulative non-monotonic sums become gauges. Gauges stay gauges.
Histograms become Prometheus histograms. Any other metric type is logged and
omitted. Names, units, help text, and labels follow the 0.66b1 exporter,
including its leading-digit sanitizing. Aerospike metric names are not
hardcoded. Scrapes are serialized so each response is one complete batch.

## Replacing it with the upstream reader

When `PrometheusMetricReader` is production-ready, remove this reader from the
provider and add the upstream reader. The upstream reader does not provide
`start_http_server`, so the application then starts and stops the scrape
endpoint itself. `OtelExporter.export` stays as it is. The swap preserves
dashboards when the scrape text matches the pinned comparison output.
