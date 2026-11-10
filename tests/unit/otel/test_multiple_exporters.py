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

import pytest

from aerospike_client_telemetry.common import ClientSnapshotRegistry
from aerospike_client_telemetry.otel import OtelExporter
from tests.fake_provider import RecordingProvider


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


def _document(marker: str) -> dict:
    return {
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "cluster_name": "local",
        "app_id": "orders-service",
        "marker": marker,
    }


_KEY = "python-sdk\n1.0.0\nlocal\norders-service"


def test_two_exporters_keep_separate_registries():
    document = _document("shared")
    snapshot = _Snapshot(document)
    orders = OtelExporter(RecordingProvider())
    billing = OtelExporter(RecordingProvider())

    orders.export(snapshot)
    billing.export(snapshot)

    assert orders is not billing
    assert orders.registry is not billing.registry
    assert orders.meter_provider is not billing.meter_provider
    assert orders.registry.current(_KEY) is document
    assert billing.registry.current(_KEY) is document
    assert orders.registry.client_keys() == (_KEY,)
    assert billing.registry.client_keys() == (_KEY,)


def _client(app_id: str, command_count: int) -> dict:
    return {
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "cluster_name": "local",
        "app_id": app_id,
        "operational_metrics_enabled": True,
        "cluster": {"command_count": command_count},
    }


def test_two_exporters_sharing_one_meter_provider_publish_both_clients():
    """The meter keeps the first callbacks, so both exporters use one registry."""
    provider = RecordingProvider()
    orders = OtelExporter(provider)
    callback = provider.meter.counters["aerospike_client_command_count"][0]
    billing = OtelExporter(provider)

    assert orders.registry is billing.registry
    assert orders.meter_provider is billing.meter_provider
    assert provider.meter.counters["aerospike_client_command_count"] == [callback]

    orders.export(_Snapshot(_client("orders", 2)))
    billing.export(_Snapshot(_client("billing", 5)))
    seen = {
        observation.attributes["app_id"]: observation.value for observation in callback(None)
    }
    assert seen == {"orders": 2, "billing": 5}


def test_a_second_registry_on_the_same_meter_provider_is_rejected():
    provider = RecordingProvider()
    OtelExporter(provider)
    with pytest.raises(ValueError, match="already has an exporter registry"):
        OtelExporter(provider, registry=ClientSnapshotRegistry())


def test_two_exporters_on_one_sdk_provider_are_both_collected():
    pytest.importorskip("opentelemetry.sdk.metrics")
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    try:
        orders = OtelExporter(provider)
        billing = OtelExporter(provider)
        assert orders.registry is billing.registry
        orders.export(_Snapshot(_client("orders", 2)))
        billing.export(_Snapshot(_client("billing", 5)))
        data = reader.get_metrics_data()
    finally:
        provider.shutdown()

    seen = {}
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name != "aerospike_client_command_count":
                    continue
                for point in metric.data.data_points:
                    seen[point.attributes["app_id"]] = point.value
    assert seen == {"orders": 2, "billing": 5}


def test_two_publishes_on_one_exporter_keep_the_latest_document():
    exporter = OtelExporter(RecordingProvider())
    exporter.export(_Snapshot(_document("first")))
    latest = _document("second")
    exporter.export(_Snapshot(latest))

    assert exporter.registry.client_keys() == (_KEY,)
    assert exporter.registry.current(_KEY) is latest
    assert exporter.registry.current(_KEY)["marker"] == "second"
