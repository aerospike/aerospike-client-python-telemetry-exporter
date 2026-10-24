# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Sync and async OpenTelemetry exporters.

``OtelExporter.export`` is a plain method for a sync cluster.
``AsyncOtelExporter.export`` is awaitable for an async cluster. Both publish
the canonical document into :class:`~aerospike_client_telemetry.common.registry.ClientSnapshotRegistry`
and return. Observable instruments on the meter provider read that registry
when the provider's reader collects, so ``export`` does not perform OTLP
network I/O and does not retain the snapshot.

OpenTelemetry is imported when this package creates a meter provider, not
when this module is imported. A meter provider supplied by the webapp is
used as given and is not shut down.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
import os
from typing import Any, Optional

from aerospike_client_telemetry.common import (
    CATALOG,
    ClientSnapshotRegistry,
    InstrumentKind,
    iter_metric_points,
    series_attributes,
)
from aerospike_client_telemetry.common.catalog import MetricDescriptor
from aerospike_client_telemetry.otel.config import (
    Compression,
    OtelExporterConfig,
    Protocol,
    Temporality,
)

_METER_NAME = "aerospike.client"
_OTEL_EXTRA = (
    "OpenTelemetry is not installed. "
    "Install the otel extra: pip install 'aerospike-client-telemetry-exporter[otel]'"
)


def _client_key(document: Mapping[str, Any]) -> str:
    """Index the document by the identity fields the client already set."""
    return "\n".join(
        str(document.get(name, ""))
        for name in ("client_type", "client_version", "cluster_name", "app_id")
    )


def _documents(registry: ClientSnapshotRegistry) -> list[Mapping[str, Any]]:
    return [
        document
        for key in registry.client_keys()
        if (document := registry.current(key)) is not None
    ]


def _observations(registry: ClientSnapshotRegistry, descriptor: MetricDescriptor):
    from opentelemetry.metrics import Observation

    for document in _documents(registry):
        for value, extra in iter_metric_points(document, descriptor):
            yield Observation(value, series_attributes(document, extra))


def _register_instruments(meter: Any, registry: ClientSnapshotRegistry) -> None:
    """Register one instrument per catalog entry the snapshot can fill."""
    for descriptor in CATALOG:
        if descriptor.source.startswith("namespace.latency."):
            continue
        callback = lambda options, descriptor=descriptor: _observations(registry, descriptor)
        kwargs = {
            "name": descriptor.name,
            "callbacks": [callback],
            "unit": descriptor.unit,
            "description": descriptor.description,
        }
        if descriptor.kind is InstrumentKind.COUNTER:
            meter.create_observable_counter(**kwargs)
        else:
            meter.create_observable_gauge(**kwargs)


def _http_metrics_endpoint(endpoint: str) -> str:
    trimmed = endpoint.rstrip("/")
    if trimmed.endswith("/v1/metrics"):
        return endpoint
    return trimmed + "/v1/metrics"


def _protocol(config: OtelExporterConfig) -> Protocol:
    if config.endpoint:
        return config.protocol
    configured = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "").strip()
    if configured in (Protocol.GRPC.value, Protocol.HTTP_PROTOBUF.value):
        return Protocol(configured)
    return config.protocol


def _preferred_temporality(choice: Temporality) -> dict[type, Any]:
    """The instrument map Java selects before building the OTLP exporter.

    Passed only when the caller set ``OtelExporterConfig.temporality``. An
    unset value is omitted so the SDK can read
    ``OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE``.
    """
    from opentelemetry.sdk.metrics import (
        Counter,
        Histogram,
        ObservableCounter,
        ObservableGauge,
        ObservableUpDownCounter,
        UpDownCounter,
    )
    from opentelemetry.sdk.metrics.export import AggregationTemporality

    cumulative = AggregationTemporality.CUMULATIVE
    delta = AggregationTemporality.DELTA
    if choice is Temporality.DELTA:
        return {
            Counter: delta,
            UpDownCounter: cumulative,
            Histogram: delta,
            ObservableCounter: delta,
            ObservableUpDownCounter: cumulative,
            ObservableGauge: cumulative,
        }
    if choice is Temporality.LOW_MEMORY:
        return {
            Counter: delta,
            UpDownCounter: cumulative,
            Histogram: delta,
            ObservableCounter: cumulative,
            ObservableUpDownCounter: cumulative,
            ObservableGauge: cumulative,
        }
    return {
        Counter: cumulative,
        UpDownCounter: cumulative,
        Histogram: cumulative,
        ObservableCounter: cumulative,
        ObservableUpDownCounter: cumulative,
        ObservableGauge: cumulative,
    }


def _otlp_exporter(config: OtelExporterConfig):
    kwargs: dict[str, Any] = {}
    if config.temporality is not None:
        kwargs["preferred_temporality"] = _preferred_temporality(config.temporality)
    if config.endpoint:
        endpoint = config.endpoint
        if _protocol(config) is Protocol.HTTP_PROTOBUF:
            endpoint = _http_metrics_endpoint(endpoint)
        kwargs["endpoint"] = endpoint
    if config.headers:
        kwargs["headers"] = dict(config.headers)
    if config.timeout != timedelta(seconds=10):
        kwargs["timeout"] = config.timeout.total_seconds()
    if config.proxy_url:
        if _protocol(config) is not Protocol.HTTP_PROTOBUF:
            raise ValueError("proxy_url is supported only with http/protobuf")
        import requests

        session = requests.Session()
        session.proxies = {"http": config.proxy_url, "https": config.proxy_url}
        kwargs["session"] = session
    try:
        if _protocol(config) is Protocol.GRPC:
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                Compression as GrpcCompression,
            )
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter,
            )

            if config.compression is Compression.GZIP:
                kwargs["compression"] = GrpcCompression.Gzip
        else:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                Compression as HttpCompression,
            )
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                OTLPMetricExporter,
            )

            if config.compression is Compression.GZIP:
                kwargs["compression"] = HttpCompression.Gzip
    except ImportError as exc:
        raise ImportError(_OTEL_EXTRA) from exc
    return OTLPMetricExporter(**kwargs)


def _build_provider(config: OtelExporterConfig):
    """Create the resource, OTLP exporter, periodic reader, and provider."""
    try:
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
    except ImportError as exc:
        raise ImportError(_OTEL_EXTRA) from exc
    if config.service_name:
        resource = Resource.create({"service.name": config.service_name})
    else:
        resource = Resource.create()
    reader_kwargs: dict[str, Any] = {}
    if config.export_interval != timedelta(seconds=60):
        reader_kwargs["export_interval_millis"] = config.export_interval.total_seconds() * 1000
    reader = PeriodicExportingMetricReader(_otlp_exporter(config), **reader_kwargs)
    return MeterProvider(metric_readers=[reader], resource=resource)


class _OtelExportState:
    """Config, registry, and publish path shared by both exporter shapes."""

    def __init__(
        self,
        config: Optional[OtelExporterConfig] = None,
        *,
        registry: Optional[ClientSnapshotRegistry] = None,
        meter_provider: Any = None,
    ) -> None:
        self.config = config if config is not None else OtelExporterConfig()
        self.registry = registry if registry is not None else ClientSnapshotRegistry()
        self._owns_provider = meter_provider is None
        if meter_provider is None:
            meter_provider = _build_provider(self.config)
        self.meter_provider = meter_provider
        self._closed = False
        _register_instruments(self.meter_provider.get_meter(_METER_NAME), self.registry)

    @property
    def owns_provider(self) -> bool:
        """Whether ``close`` shuts the meter provider down.

        True when this exporter created the provider. False when the webapp
        passed one in.
        """
        return self._owns_provider

    def publish(self, snapshot: Any) -> None:
        """Store the canonical document and drop the snapshot."""
        if self._closed:
            raise RuntimeError("exporter is closed")
        document = snapshot.to_canonical_dict()
        self.registry.replace(_client_key(document), document)

    def close(self) -> None:
        """Release exporter-owned resources. The client SDK does not call this.

        A provider this exporter created is shut down. A provider the webapp
        passed in is left running.
        """
        self._closed = True
        if not self._owns_provider:
            return
        provider = self.meter_provider
        self._owns_provider = False
        provider.shutdown()


class OtelExporter(_OtelExportState):
    """Sync OpenTelemetry exporter for ``aerospike_sdk.sync.Cluster``."""

    def export(self, snapshot: Any) -> None:
        """Publish one snapshot to the registry."""
        self.publish(snapshot)


class AsyncOtelExporter(_OtelExportState):
    """Async OpenTelemetry exporter for ``aerospike_sdk.Cluster``."""

    async def export(self, snapshot: Any) -> None:
        """Publish one snapshot to the registry."""
        self.publish(snapshot)
