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
from datetime import timedelta

import pytest

from aerospike_sdk.metrics.export import check_exporter
from aerospike_client_telemetry.common import MetricCatalog
from aerospike_client_telemetry.common.catalog import InstrumentKind, MetricDescriptor
from aerospike_client_telemetry.otel import (
    AsyncOtelExporter,
    Compression,
    OtelExporter,
    OtelExporterConfig,
    Protocol,
    Temporality,
)
from aerospike_client_telemetry.otel.config import OtelExporterConfig as ConfigFromModule


def test_importing_otel_does_not_import_opentelemetry():
    import subprocess

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import aerospike_client_telemetry.otel; "
            "assert not any(name == 'opentelemetry' or name.startswith('opentelemetry.') for name in sys.modules)",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_root_package_does_not_export_the_otel_backend():
    import aerospike_client_telemetry

    assert "OtelExporter" not in aerospike_client_telemetry.__all__


def test_config_is_immutable_and_copies_headers():
    headers = {"authorization": "secret"}
    config = OtelExporterConfig(
        service_name="checkout",
        endpoint="http://127.0.0.1:4318/v1/metrics",
        protocol="http/protobuf",
        headers=headers,
        timeout=timedelta(seconds=5),
        compression=Compression.GZIP,
        temporality="cumulative",
    )
    headers["authorization"] = "changed"
    assert config.headers == {"authorization": "secret"}
    assert config.protocol is Protocol.HTTP_PROTOBUF
    assert config.temporality is Temporality.CUMULATIVE
    assert isinstance(config, ConfigFromModule)
    empty = OtelExporterConfig()
    assert empty.service_name == ""
    assert empty.endpoint == ""
    assert empty.temporality is None
    with pytest.raises(ValueError, match="timeout"):
        OtelExporterConfig(service_name="checkout", endpoint="http://127.0.0.1:4318", timeout=timedelta(0))
    with pytest.raises(ValueError, match="export_interval"):
        OtelExporterConfig(
            service_name="checkout",
            endpoint="http://127.0.0.1:4318",
            export_interval=timedelta(0),
        )


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
        "conn_type": "sync",
        "cluster_name": "local",
        "app_id": "checkout",
    }
    snapshot = _Snapshot(document)
    config = OtelExporterConfig(service_name="checkout", endpoint="http://127.0.0.1:4318")
    exporter = OtelExporter(config)
    check_exporter(exporter, awaitable=False)
    exporter.export(snapshot)
    stored = exporter.registry.current("python-sdk\n1.0.0\nsync\nlocal\ncheckout")
    assert stored is document
    assert stored["app_id"] == "checkout"
    exporter.close()
    with pytest.raises(RuntimeError, match="closed"):
        exporter.export(snapshot)


@pytest.mark.asyncio
async def test_async_otel_exporter_matches_the_async_protocol():
    config = OtelExporterConfig(service_name="checkout", endpoint="http://127.0.0.1:4318")
    exporter = AsyncOtelExporter(config)
    check_exporter(exporter, awaitable=True)
    await exporter.export(
        _Snapshot({"client_type": "python-sdk", "conn_type": "async", "app_id": "checkout"})
    )
    assert exporter.registry.client_keys() == ("python-sdk\n\nasync\n\ncheckout",)
    exporter.close()


def test_catalog_rejects_a_conflicting_descriptor():
    catalog = MetricCatalog()
    catalog.register(MetricDescriptor("aerospike.client.commands", InstrumentKind.COUNTER, "commands"))
    catalog.register(MetricDescriptor("aerospike.client.commands", InstrumentKind.COUNTER, "commands"))
    with pytest.raises(ValueError, match="already registered"):
        catalog.register(MetricDescriptor("aerospike.client.commands", InstrumentKind.GAUGE, "commands"))
    assert len(catalog) == 1
