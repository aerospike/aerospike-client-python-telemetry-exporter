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

"""The periodic reader and the client push do not share a clock.

``export`` replaces the one stored snapshot. ``PeriodicExportingMetricReader``
collects that snapshot on its own interval. A reader at half the client
interval (15s against a 30s push) reports the same absolute values again.
A reader at twice the client interval (60s against a 30s push) reports only
the latest snapshot; the one it skipped is already gone.
"""

from __future__ import annotations

import threading
import time

import pytest

from aerospike_client_telemetry.otel import OtelExporter

pytest.importorskip("opentelemetry.sdk.metrics")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExportResult,
    MetricExporter,
    PeriodicExportingMetricReader,
)

_COMMAND = "aerospike_client_command_count"
_NODES = "aerospike_client_total_nodes"
_WAIT_SECONDS = 3


class _Snapshot:
    def __init__(self, document):
        self.document = document

    def to_canonical_dict(self):
        return self.document


class _Capture(MetricExporter):
    """Records each periodic collect. The reader calls this from its thread."""

    def __init__(self) -> None:
        super().__init__()
        self._batches: list = []
        self._lock = threading.Lock()

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        with self._lock:
            self._batches.append(metrics_data)
        return MetricExportResult.SUCCESS

    def shutdown(self, timeout_millis=30_000, **kwargs):
        return None

    def force_flush(self, timeout_millis=10_000):
        return True

    def readings(self) -> list[dict[str, float]]:
        with self._lock:
            batches = list(self._batches)
        return [row for batch in batches if (row := _reading(batch))]


def _document(command_count: int, total_nodes: int, stamp: str) -> _Snapshot:
    return _Snapshot(
        {
            "timestamp": stamp,
            "client_type": "python-sdk",
            "client_version": "1.0.0",
            "cluster_name": "local",
            "app_id": "interval",
            "operational_metrics_enabled": True,
            "command_count": command_count,
            "total_nodes": total_nodes,
        }
    )


def _reading(metrics_data) -> dict[str, float] | None:
    found: dict[str, float] = {}
    for resource in metrics_data.resource_metrics:
        for scope in resource.scope_metrics:
            if scope.scope.name != "aerospike.client":
                continue
            for metric in scope.metrics:
                if metric.name not in (_COMMAND, _NODES):
                    continue
                points = list(metric.data.data_points)
                if points:
                    found[metric.name] = float(points[0].value)
    if _COMMAND not in found or _NODES not in found:
        return None
    return found


def _wait(capture: _Capture, predicate) -> list[dict[str, float]]:
    deadline = time.monotonic() + _WAIT_SECONDS
    last: list[dict[str, float]] = []
    while time.monotonic() < deadline:
        last = capture.readings()
        if predicate(last):
            return last
        time.sleep(0.02)
    raise AssertionError(f"reader did not reach the expected collects: {last}")


def _provider(interval_millis: int) -> tuple[_Capture, MeterProvider, OtelExporter]:
    capture = _Capture()
    reader = PeriodicExportingMetricReader(
        capture,
        export_interval_millis=interval_millis,
    )
    provider = MeterProvider(metric_readers=[reader])
    return capture, provider, OtelExporter(provider)


def test_a_faster_reader_repeats_the_same_absolute_values():
    """Reader every 15s, client every 30s: the second collect repeats the first."""
    capture, provider, exporter = _provider(200)
    try:
        exporter.export(_document(4, 2, "2026-01-01T00:00:00+00:00"))
        repeated = _wait(capture, lambda rows: [row[_COMMAND] for row in rows].count(4) >= 2)
        assert all(row[_NODES] == 2 for row in repeated if row[_COMMAND] == 4)

        exporter.export(_document(9, 3, "2026-01-01T00:00:30+00:00"))
        moved = _wait(capture, lambda rows: any(row[_COMMAND] == 9 for row in rows))
        counts = [row[_COMMAND] for row in moved]
        assert counts.count(4) >= 2
        assert 9 in counts
        assert set(counts) <= {4, 9}
        assert {(row[_COMMAND], row[_NODES]) for row in moved} <= {(4, 2), (9, 3)}
    finally:
        exporter.close()
        provider.shutdown()


def test_a_slower_reader_publishes_only_the_latest_snapshot():
    """Reader every 60s, client every 30s: the collect in between is overwritten."""
    capture, provider, exporter = _provider(500)
    try:
        exporter.export(_document(4, 1, "2026-01-01T00:00:00+00:00"))
        exporter.export(_document(7, 2, "2026-01-01T00:00:30+00:00"))
        exporter.export(_document(11, 4, "2026-01-01T00:01:00+00:00"))
        first = _wait(capture, lambda rows: bool(rows))
        assert [row[_COMMAND] for row in first] == [11]
        assert first[0][_NODES] == 4

        exporter.export(_document(15, 5, "2026-01-01T00:01:30+00:00"))
        exporter.export(_document(20, 6, "2026-01-01T00:02:00+00:00"))
        later = _wait(capture, lambda rows: any(row[_COMMAND] == 20 for row in rows))
        counts = [row[_COMMAND] for row in later]
        nodes = [row[_NODES] for row in later]
        assert 4 not in counts
        assert 7 not in counts
        assert 15 not in counts
        assert 11 in counts
        assert 20 in counts
        assert 1 not in nodes
        assert 2 not in nodes
        assert 5 not in nodes
        assert 4 in nodes
        assert 6 in nodes
    finally:
        exporter.close()
        provider.shutdown()
