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

"""Snapshot translation shared by every backend.

Backends read the canonical document the Python client already produces.
This package owns the metric catalog and the per-client registry a backend
consults when it reports. Identity, node, and namespace fields stay on that
document. It does not import OpenTelemetry or Prometheus.
"""

from aerospike_client_telemetry.common.catalog import (
    CATALOG,
    LABEL_MAP,
    METRIC_MAP,
    InstrumentKind,
    MetricCatalog,
    MetricDescriptor,
    iter_metric_points,
    rename_label,
    series_attributes,
    client_app_ip,
)
from aerospike_client_telemetry.common.registry import ClientSnapshotRegistry

__all__ = [
    "CATALOG",
    "LABEL_MAP",
    "METRIC_MAP",
    "ClientSnapshotRegistry",
    "InstrumentKind",
    "MetricCatalog",
    "MetricDescriptor",
    "iter_metric_points",
    "rename_label",
    "series_attributes",
    "client_app_ip",
]
