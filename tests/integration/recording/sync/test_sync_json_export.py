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

"""Recording exporter plus a JSON file of the canonical snapshot.

The recording exporter keeps its small in-memory record. A second exporter
writes each canonical document to ``build/recording-snapshot.json`` so the
full snapshot can be opened after the run. Both wait on the real scheduler.
"""

from __future__ import annotations

import json
from pathlib import Path
import time

import pytest

from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry import RecordingExporter
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    custom_metrics_settings,
    dataset,
    seed_host,
)

pytestmark = pytest.mark.integration

_OUTPUT = Path(__file__).resolve().parents[4] / "build" / "recording-snapshot.json"


class JsonFileExporter:
    """Write each canonical snapshot document to one JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self.documents: list[dict] = []

    def export(self, snapshot) -> None:
        document = snapshot.to_canonical_dict()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        documents = [*self.documents, document]
        temporary = self._path.with_suffix(self._path.suffix + ".tmp")
        temporary.write_text(json.dumps(documents, indent=2) + "\n")
        temporary.replace(self._path)
        self.documents.append(document)

    def close(self) -> None:
        return None


def test_recording_exporter_and_json_file_receive_the_same_snapshot(capsys):
    host, port = seed_host()
    definition = (
        ClusterDefinition(host, port)
        .app_id("telemetry-exporter-itest")
        .with_system_settings(custom_metrics_settings())
    )
    try:
        cluster = definition.connect()
    except Exception as exc:
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    recording = RecordingExporter()
    writer = JsonFileExporter(_OUTPUT)
    try:
        cluster.add_exporter(recording)
        cluster.add_exporter(writer)
        cluster.enable_metrics(
            MetricsPolicy(operational_enabled=True, usage_enabled=True)
        )
        session = cluster.create_session()
        ds = dataset()
        session.upsert(ds.id(1)).put({"n": 1}).execute()

        deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
        document = None
        while time.monotonic() < deadline:
            if recording.callbacks >= 1 and writer.documents and _OUTPUT.is_file():
                try:
                    document = json.loads(_OUTPUT.read_text())[0]
                except json.JSONDecodeError:
                    document = None
                else:
                    break
            time.sleep(0.05)
        else:
            raise AssertionError("periodic scheduler did not call export()")

        record = recording.records[0]
        assert record.client_type == "python-sdk"
        assert record.app_id == "telemetry-exporter-itest"
        assert record.node_names
        assert document["client_type"] == record.client_type
        assert document["app_id"] == record.app_id
        assert document["operational_metrics_enabled"] is True
        assert document["usage_metrics_enabled"] is True
        assert document["cluster"]["command_count"] >= 1
        assert document["usage"]["feature.api.blocking"] >= 1
        assert document["usage"]["feature.shape.point"] >= 1
        with capsys.disabled():
            print(f"wrote {_OUTPUT}")
    finally:
        cluster.disable_metrics()
        cluster.close()
        recording.close()
        writer.close()
