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

"""Two OTel exporters, and one exporter receiving two publishes."""

from aerospike_client_telemetry.otel import OtelExporter, OtelExporterConfig


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


def _config(service_name: str) -> OtelExporterConfig:
    return OtelExporterConfig(
        service_name=service_name,
        endpoint=f"http://127.0.0.1:4318/v1/metrics/{service_name}",
    )


def _document(marker: str) -> dict:
    return {
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "conn_type": "sync",
        "cluster_name": "local",
        "app_id": "orders-service",
        "marker": marker,
    }


_KEY = "python-sdk\n1.0.0\nsync\nlocal\norders-service"


def test_two_exporters_keep_separate_registries():
    document = _document("shared")
    snapshot = _Snapshot(document)
    orders = OtelExporter(_config("orders"))
    billing = OtelExporter(_config("billing"))

    try:
        orders.export(snapshot)
        billing.export(snapshot)

        assert orders is not billing
        assert orders.registry is not billing.registry
        assert orders.config.service_name == "orders"
        assert billing.config.service_name == "billing"
        assert orders.registry.current(_KEY) is document
        assert billing.registry.current(_KEY) is document
        assert orders.registry.client_keys() == (_KEY,)
        assert billing.registry.client_keys() == (_KEY,)
    finally:
        orders.close()
        billing.close()


def test_two_publishes_on_one_exporter_keep_the_latest_document():
    exporter = OtelExporter(_config("orders"))
    try:
        exporter.export(_Snapshot(_document("first")))
        latest = _document("second")
        exporter.export(_Snapshot(latest))

        assert exporter.registry.client_keys() == (_KEY,)
        assert exporter.registry.current(_KEY) is latest
        assert exporter.registry.current(_KEY)["marker"] == "second"
    finally:
        exporter.close()
