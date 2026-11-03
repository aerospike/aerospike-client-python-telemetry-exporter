# Prometheus scrape reader

Status: design, not implemented. Revisit this before writing the reader.

The production exporter does not use `PrometheusMetricReader`. The package
`opentelemetry-exporter-prometheus` 0.66b1 is Development Status 4, Beta.
It is not in `pyproject.toml` and not in the `[otel]` extra. Unit and
integration tests import it, and CI installs it only for those tests.

## Decision

Extend `opentelemetry.sdk.metrics.export.MetricReader`. Do not extend
`PrometheusMetricReader`. A subclass of the beta class would still ship the
beta package.

`collect()` on `MetricReader` is final. The subclass implements
`_receive_metrics(metrics_data)`. The webapp adds this reader to the same
`MeterProvider` that already has the periodic OTLP reader and the observable
instruments `OtelExporter` registered. A scrape calls `collect()`. The SDK
runs the callbacks already on that provider and passes one `MetricsData`
batch to `_receive_metrics`. Instruments are not registered a second time.

`prometheus_client` is the stable library that writes Prometheus text and can
serve the scrape port. This package does not import
`opentelemetry.exporter.prometheus`. This package maps `MetricsData` into
`prometheus_client` metric families.

## Dependencies

The reader subclasses `MetricReader`, so importing it requires
`opentelemetry-sdk`. That library is already the optional `[otel]` extra. It
does not become a dependency of every install. The package root stays free of
OpenTelemetry. A recording-only install does not import the reader.

`prometheus_client` is a separate library from the OpenTelemetry packages. The
scrape reader needs it. It is not part of the `[otel]` extra today. Add it to
the extra that installs the reader, and import the reader from its own module
so the OTLP exporter does not load it.

## Constructor

Match `PrometheusMetricReader` so a later replacement is a class-name and
import change. The `MeterProvider` line stays the same.

| Argument | Default | Role |
| --- | --- | --- |
| `disable_target_info` | `False` | When false, the scrape includes one `target_info` series. Its labels are the OpenTelemetry resource attributes (service name, and the rest of the resource). When true, that series is omitted. Aerospike counters and gauges are unaffected. |
| `prefix` | `""` | A non-empty value is placed in front of every exported name, with an underscore. It applies to every instrument on the provider, including the webapp's own instruments. |
| `scope_info_enabled` | `True` | When true, each sample carries `otel_scope_name`, `otel_scope_version`, and `otel_scope_schema_url`. Client instruments use the meter `aerospike.client`. When false, those labels are omitted. |
| `resource_attribute_filter` | `None` | Keyword-only. With the default, resource attributes stay on `target_info` only. A callback `(name) -> bool` copies each selected resource attribute onto every sample. The label name is sanitized. |
| `registry` | process-global `REGISTRY` | Keyword-only. The `prometheus_client` registry this reader registers on. |

There is no `port` argument. The webapp starts the server:

```python
from prometheus_client import start_http_server

start_http_server(port, registry=reader_registry)
```

`reader_registry` is the same object passed to the reader. The OTLP periodic
reader and `OtelExporter` stay as they are.

## Process-global registry

`prometheus_client` creates `REGISTRY` when it is imported. The application
does not set it. Omitting the `registry` argument registers this reader on
that object. `start_http_server(port)` with no registry argument serves it,
together with anything else that used the default, including the process
collectors `prometheus_client` adds there.

If nothing serves `REGISTRY`, the series sit in it and no port returns them.
`generate_latest(REGISTRY)` can still render the text.

Passing `registry=None` fails when the reader calls `register`. Passing a new
`CollectorRegistry()` keeps this reader off the global one. The HTTP server
must be started with that same object. A server started without it keeps
serving `REGISTRY`, and this reader's series are absent from the scrape.

One process can bind one scrape port. Another worker needs its own port.

## Prometheus notation

Follow the OpenTelemetry Prometheus compatibility rules. Do not invent a
second naming scheme. [reference.md](reference.md) states the same rules for
another language.

- A metric name uses Prometheus characters only. `.` becomes `_`, any other
  illegal character becomes `_`, and a leading digit is prefixed with `_`.
  `aerospike.client.cluster.total_nodes` is
  `aerospike_client_cluster_total_nodes`.
- A monotonic counter gains `_total`. A gauge does not.
  `aerospike.client.cluster.command_count` is
  `aerospike_client_cluster_command_count_total`.
- Brace units such as `{connection}` are removed and are not appended to the
  name. A unit Prometheus does append, such as bytes, follows the
  compatibility mapping.
- Label names are sanitized the same way. Labels this exporter already
  publishes keep their spellings: `cluster`, `client_type`,
  `client_version`, `app_id`, `client_app_ip`, `server_node_id`,
  `server_node_address`, `server_node_port`, `ns`, `latency_type`,
  `latency_unit`, and `le`.
- Latency stays a cumulative counter with an `le` label, including `+Inf`.
  It is not rewritten as a Prometheus histogram.
- Each family has a `TYPE` line and a `HELP` line. Values are the cumulative
  totals from that scrape, not deltas.

## Replacing it when PrometheusMetricReader is production-ready

The webapp removes this reader from the provider and adds
`PrometheusMetricReader` with the same constructor arguments. It serves the
scrape with `prometheus_client.start_http_server` and the same registry.
`OtelExporter` still owns instrument registration. The OTLP reader stays.

The swap is only that class change when:

- This reader is only a `MetricReader`. It does not create instruments and it
  does not wrap `OtelExporter`.
- The scrape text matches what `PrometheusMetricReader` produces, so
  dashboards and alerts keep working.
- The port and registry are chosen by the webapp, outside `OtelExporter`.
