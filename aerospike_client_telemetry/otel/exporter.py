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
create those objects and does not shut the provider down.
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
        self._closed = False
        _register_instruments(self.meter_provider.get_meter(_METER_NAME), self.registry)

    def publish(self, snapshot: Any) -> None:
        """Store the canonical document and drop the snapshot."""
        if self._closed:
            raise RuntimeError("exporter is closed")
        document = snapshot.to_canonical_dict()
        self.registry.replace(_client_key(document), document)

    def close(self) -> None:
        """Mark the exporter closed. The client SDK does not call this.

        The webapp's meter provider is left running.
        """
        self._closed = True


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
