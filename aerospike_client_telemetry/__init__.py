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

"""External exporters for Aerospike Python client metrics snapshots.

The client SDK collects metrics and pushes a snapshot. This package translates
that snapshot.

* :mod:`aerospike_client_telemetry.common` holds the shared catalog and the
  per-client registry. Label fields stay on the canonical document.
* :mod:`aerospike_client_telemetry.recording` records callbacks in memory.
* :mod:`aerospike_client_telemetry.otel` is the OpenTelemetry backend. Import it
  directly; this module does not load it.
"""

try:
    from importlib.metadata import version as _meta_version

    __version__ = _meta_version("aerospike-client-telemetry-exporter")
except Exception:
    __version__ = "0.0.0"

from aerospike_client_telemetry.recording import (
    AsyncRecordingExporter,
    RecordedExport,
    RecordingExporter,
    translate_snapshot,
)

__all__ = [
    "AsyncRecordingExporter",
    "RecordedExport",
    "RecordingExporter",
    "translate_snapshot",
]
