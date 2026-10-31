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

"""Unit tests for the recording exporter contract."""

import copy
import inspect

import pytest

from aerospike_sdk.metrics.export import check_exporter
from aerospike_client_telemetry import (
    AsyncRecordingExporter,
    RecordingExporter,
    translate_snapshot,
)


def _document():
    return {
        "timestamp": "2026-01-01T00:00:00+00:00",
        "client_type": "python-sdk",
        "client_version": "1.0.0",
        "cluster_name": "local",
        "app_id": "telemetry-exporter",
        "labels": {"owner": "platform"},
        "metrics_enabled": True,
        "operational_metrics_enabled": True,
        "usage_metrics_enabled": False,
        "latency_unit": "milliseconds",
        "latency_columns": 7,
        "latency_shift": 1,
        "total_nodes": 1,
        "invalid_node_count": 0,
        "command_count": 4,
        "command_retries": 1,
        "nodes": [
            {
                "node_name": "BB9",
                "node_address": "127.0.0.1",
                "node_port": 3100,
                "namespaces": [{"namespace": "test", "errors": 0}],
            }
        ],
        "nodes_departed": [],
    }


class _Snapshot:
    def __init__(self, document):
        self.document = document
        self.canonical_calls = 0

    def to_canonical_dict(self):
        self.canonical_calls += 1
        return self.document


def _contains(root, target) -> bool:
    seen = set()

    def walk(value) -> bool:
        if value is target:
            return True
        identity = id(value)
        if identity in seen:
            return False
        if isinstance(value, (str, bytes, int, float, bool, type(None))):
            return False
        seen.add(identity)
        if isinstance(value, dict):
            return any(walk(item) for item in value.values()) or any(
                walk(item) for item in value.keys()
            )
        if isinstance(value, (list, tuple, set)):
            return any(walk(item) for item in value)
        state = getattr(value, "__dict__", None)
        if isinstance(state, dict):
            return walk(state)
        return False

    return walk(root)


def test_sync_exporter_has_plain_export():
    exporter = RecordingExporter()
    assert inspect.iscoroutinefunction(exporter.export) is False
    check_exporter(exporter, awaitable=False)


def test_async_exporter_has_awaitable_export():
    exporter = AsyncRecordingExporter()
    assert inspect.iscoroutinefunction(exporter.export) is True
    check_exporter(exporter, awaitable=True)


def test_translate_snapshot_reads_without_mutating():
    document = _document()
    original = copy.deepcopy(document)

    record = translate_snapshot(document)

    assert document == original
    assert record.client_type == "python-sdk"
    assert record.app_id == "telemetry-exporter"
    assert record.node_names == ("BB9",)
    assert record.command_count == 4
    assert record.labels["cluster"] == "local"
    assert record.labels["owner"] == "platform"
    assert "cluster_name" not in record.labels
    document["nodes"].append({"node_name": "later"})
    document["command_count"] = 99
    assert record.node_names == ("BB9",)
    assert record.command_count == 4


def test_export_translates_without_retaining_the_snapshot():
    document = _document()
    snapshot = _Snapshot(document)
    exporter = RecordingExporter()

    exporter.export(snapshot)

    assert snapshot.canonical_calls == 1
    assert exporter.callbacks == 1
    assert exporter.records[0].client_type == "python-sdk"
    assert _contains(exporter, snapshot) is False
    assert _contains(exporter, document) is False
    assert exporter.closed is False


@pytest.mark.asyncio
async def test_async_export_translates_without_retaining_the_snapshot():
    document = _document()
    snapshot = _Snapshot(document)
    exporter = AsyncRecordingExporter()

    await exporter.export(snapshot)

    assert snapshot.canonical_calls == 1
    assert exporter.callbacks == 1
    assert exporter.records[0].node_names == ("BB9",)
    assert _contains(exporter, snapshot) is False
    assert _contains(exporter, document) is False


def test_injected_failure_does_not_record():
    exporter = RecordingExporter(fail_times=1)
    with pytest.raises(RuntimeError, match="injected exporter failure"):
        exporter.export(_Snapshot(_document()))
    assert exporter.callbacks == 0
    assert exporter.errors == 1
    assert exporter.records == ()
