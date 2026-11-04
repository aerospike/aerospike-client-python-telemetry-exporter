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
the canonical document into
:class:`~aerospike_client_telemetry.common.registry.ClientSnapshotRegistry`
and return. Observable instruments on the webapp's meter provider read that
registry when its reader collects, so ``export`` does not perform OTLP
network I/O and does not retain the snapshot.

The webapp builds the meter provider, including its resource, OTLP exporter,
and periodic reader, and passes that provider in. This package does not
create that provider and does not shut it down. ``start_http_server`` is the
exception: it serves the provider's Prometheus reader over plaintext, TLS, or
mutual TLS, and ``provider.shutdown()`` stops that server. A webapp that
serves the reader's registry itself does not call ``start_http_server``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional

from aerospike_client_telemetry.common import (
    CATALOG,
    ClientSnapshotRegistry,
    InstrumentKind,
    iter_metric_points,
    series_attributes,
)
from aerospike_client_telemetry.common.catalog import MetricDescriptor

_METER_NAME = "aerospike.client"
_UNSET = object()


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


def _prometheus_readers(meter_provider: Any) -> list[Any]:
    readers: list[Any] = []
    seen: set[int] = set()
    pools = []
    metric_readers = getattr(meter_provider, "_metric_readers", None)
    if metric_readers:
        pools.append(metric_readers)
    consumer = getattr(meter_provider, "_measurement_consumer", None)
    storages = getattr(consumer, "_reader_storages", None)
    if storages:
        pools.append(storages)
    from aerospike_client_telemetry.otel.prometheus_reader import CustomPrometheusMetricReader

    for pool in pools:
        for candidate in pool:
            if isinstance(candidate, CustomPrometheusMetricReader) and id(candidate) not in seen:
                seen.add(id(candidate))
                readers.append(candidate)
    return readers


def _only_prometheus_reader(meter_provider: Any) -> Any:
    readers = _prometheus_readers(meter_provider)
    if len(readers) == 1:
        return readers[0]
    if not readers:
        raise RuntimeError(
            "start_http_server requires a CustomPrometheusMetricReader on the MeterProvider"
        )
    raise RuntimeError(
        "MeterProvider has more than one CustomPrometheusMetricReader; "
        "pass reader= to start_http_server"
    )


def _register_instruments(meter: Any, registry: ClientSnapshotRegistry) -> None:
    """Register one instrument per catalog entry the snapshot can fill."""
    for descriptor in CATALOG:
        def callback(options, descriptor=descriptor):
            return _observations(registry, descriptor)

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


class _OtelExportState:
    """Registry and publish path shared by both exporter shapes."""

    def __init__(
        self,
        meter_provider: Any,
        *,
        registry: Optional[ClientSnapshotRegistry] = None,
    ) -> None:
        if meter_provider is None:
            raise TypeError("meter_provider is required")
        self.meter_provider = meter_provider
        self.registry = registry if registry is not None else ClientSnapshotRegistry()
        self._prometheus_http_server: Any = None
        _register_instruments(self.meter_provider.get_meter(_METER_NAME), self.registry)

    def start_http_server(
        self,
        host: Any = _UNSET,
        port: Any = _UNSET,
        tls_config: Any = _UNSET,
        *,
        config: Any = None,
        reader: Any = None,
    ) -> Any:
        """Start the Prometheus scrape server for this provider's reader.

        ``host``, ``port``, and ``tls_config`` are server settings. They are
        not reader settings. ``tls_config`` selects plaintext, TLS, or mutual
        TLS. The server uses the ``CustomPrometheusMetricReader`` registry.
        ``provider.shutdown()`` stops it. Pass ``reader`` when the provider
        has more than one Prometheus reader. A webapp that starts
        ``prometheus_client.start_http_server`` on this registry does not call
        this method, and ``provider.shutdown()`` does not stop that server.
        """
        from aerospike_client_telemetry.otel.prometheus_reader import (
            CustomPrometheusMetricReader,
            PrometheusHTTPServer,
        )

        chosen = reader if reader is not None else _only_prometheus_reader(self.meter_provider)
        if not isinstance(chosen, CustomPrometheusMetricReader):
            raise TypeError("reader must be a CustomPrometheusMetricReader")
        if chosen not in _prometheus_readers(self.meter_provider):
            raise ValueError("reader is not registered on this exporter's MeterProvider")
        if self._prometheus_http_server is not None:
            raise RuntimeError("Prometheus HTTP server is already started")
        if config is not None and any(value is not _UNSET for value in (host, port, tls_config)):
            raise TypeError("pass config or host, port, and tls_config, not both")
        if config is not None:
            server = PrometheusHTTPServer(chosen.registry, config=config)
        else:
            server_args: dict[str, Any] = {}
            if host is not _UNSET:
                server_args["host"] = host
            if port is not _UNSET:
                server_args["port"] = port
            if tls_config is not _UNSET:
                server_args["tls_config"] = tls_config
            server = PrometheusHTTPServer(chosen.registry, **server_args)
        try:
            server.start()
            chosen._attach_http_server(server)
        except Exception:
            server.shutdown()
            raise
        self._prometheus_http_server = server
        return server

    def publish(self, snapshot: Any) -> None:
        """Store the canonical document and drop the snapshot."""
        document = snapshot.to_canonical_dict()
        self.registry.replace(_client_key(document), document)


class OtelExporter(_OtelExportState):
    """Sync OpenTelemetry exporter for ``aerospike_sdk.sync.Cluster``."""

    def export(self, snapshot: Any) -> None:
        """Publish one snapshot to the registry."""
        self.publish(snapshot)


class AsyncOtelExporter(_OtelExportState):
    """Async OpenTelemetry exporter for ``aerospike_sdk.Cluster``.

    The client runs this on its metrics-owned event loop. ``export`` only
    copies the canonical document into the registry. It does not use asyncio
    objects from the application's event loop.
    """

    async def export(self, snapshot: Any) -> None:
        """Publish one snapshot to the registry."""
        self.publish(snapshot)
