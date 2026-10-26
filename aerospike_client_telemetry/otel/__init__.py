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

"""OpenTelemetry backend.

:class:`~aerospike_client_telemetry.otel.exporter.OtelExporter` and
:class:`~aerospike_client_telemetry.otel.exporter.AsyncOtelExporter` register
instruments on a meter provider the webapp supplies. Importing this package
does not import OpenTelemetry. The webapp's provider is what imports it.
"""

from aerospike_client_telemetry.otel.exporter import AsyncOtelExporter, OtelExporter

__all__ = [
    "AsyncOtelExporter",
    "OtelExporter",
]
