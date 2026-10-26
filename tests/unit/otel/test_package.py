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

"""The OTel package is importable before the OpenTelemetry extra is installed."""

import sys

import pytest

from aerospike_sdk.metrics.export import check_exporter
from aerospike_client_telemetry.common import MetricCatalog
from aerospike_client_telemetry.common.catalog import InstrumentKind, MetricDescriptor
from aerospike_client_telemetry.otel import AsyncOtelExporter, OtelExporter
from tests.fake_provider import RecordingProvider


def test_importing_otel_does_not_import_opentelemetry():
    import subprocess

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import aerospike_client_telemetry.otel; "
            "assert not any(name == 'opentelemetry' or "
            "name.startswith('opentelemetry.') for name in sys.modules)",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_root_package_does_not_export_the_otel_backend():
    import aerospike_client_telemetry

    assert "OtelExporter" not in aerospike_client_telemetry.__all__


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


def test_otel_exporters_publish_without_retaining_the_snapshot():
    document = {
        "timestamp": "2026-01-01T00:00:00+00:00",
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "cluster_name": "local",
        "app_id": "checkout",
    }
    snapshot = _Snapshot(document)
    exporter = OtelExporter(RecordingProvider())
    check_exporter(exporter, awaitable=False)
    exporter.export(snapshot)
    stored = exporter.registry.current("python-sdk\n1.0.0\nlocal\ncheckout")
    assert stored is document
    assert stored["app_id"] == "checkout"
    exporter.close()
    with pytest.raises(RuntimeError, match="closed"):
        exporter.export(snapshot)


@pytest.mark.asyncio
async def test_async_otel_exporter_matches_the_async_protocol():
    exporter = AsyncOtelExporter(RecordingProvider())
    check_exporter(exporter, awaitable=True)
    await exporter.export(
        _Snapshot({"client_type": "python-sdk", "app_id": "checkout"})
    )
    assert exporter.registry.client_keys() == ("python-sdk\n\n\ncheckout",)
    exporter.close()


def test_catalog_rejects_a_conflicting_descriptor():
    catalog = MetricCatalog()
    counter = MetricDescriptor(
        "aerospike.client.commands", InstrumentKind.COUNTER, "commands"
    )
    catalog.register(counter)
    catalog.register(counter)
    with pytest.raises(ValueError, match="already registered"):
        catalog.register(
            MetricDescriptor(
                "aerospike.client.commands", InstrumentKind.GAUGE, "commands"
            )
        )
    assert len(catalog) == 1
