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

"""Resolved OpenTelemetry exporter settings.

This module has no OpenTelemetry import. Instrument setup and OTLP transport
belong to :mod:`aerospike_client_telemetry.otel.exporter`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Mapping, Optional


class Protocol(str, Enum):
    """OTLP metrics transport."""

    GRPC = "grpc"
    HTTP_PROTOBUF = "http/protobuf"


class Compression(str, Enum):
    """OTLP payload compression."""

    NONE = "none"
    GZIP = "gzip"


class Temporality(str, Enum):
    """Aggregation temporality preference passed to the OTel SDK."""

    CUMULATIVE = "cumulative"
    DELTA = "delta"
    LOW_MEMORY = "lowmemory"


@dataclass(frozen=True)
class OtelExporterConfig:
    """Immutable settings for one OpenTelemetry exporter.

    Empty ``service_name`` and ``endpoint`` mean the OpenTelemetry
    environment variables supply them. Set either one only to override that
    environment. ``headers`` is copied so a later change to the caller's
    mapping does not affect the exporter. ``timeout`` is the deadline for one
    OTLP send, including the OpenTelemetry client's retries. There is no
    retry-count field.
    ``export_interval`` is the OTel SDK collection interval, which is separate
    from the Aerospike client's push interval. ``temporality`` is passed to
    the OTLP exporter only when set. Left unset, the exporter follows
    ``OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE``, which defaults to
    cumulative. ``proxy_url`` is applied to the HTTP session. It is rejected
    for gRPC. A webapp that passes its own meter provider does not use this
    destination.
    """

    service_name: str = ""
    endpoint: str = ""
    protocol: Protocol = Protocol.HTTP_PROTOBUF
    headers: Mapping[str, str] = field(default_factory=dict)
    timeout: timedelta = timedelta(seconds=10)
    export_interval: timedelta = timedelta(seconds=60)
    compression: Compression = Compression.NONE
    temporality: Optional[Temporality] = None
    proxy_url: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "protocol", Protocol(self.protocol))
        object.__setattr__(self, "compression", Compression(self.compression))
        if self.temporality is not None:
            object.__setattr__(self, "temporality", Temporality(self.temporality))
        object.__setattr__(self, "headers", dict(self.headers))
        if self.timeout.total_seconds() <= 0:
            raise ValueError("timeout must be positive")
        if self.export_interval.total_seconds() <= 0:
            raise ValueError("export_interval must be positive")
