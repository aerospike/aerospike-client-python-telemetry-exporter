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

"""In-memory exporters that record one periodic callback.

Each ``export`` call reads ``snapshot.to_canonical_dict()`` immediately and
keeps only a small derived record. The snapshot and the document are not
retained after the call returns. The application owns ``close()``; the client
SDK does not call it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import threading
from typing import Any, Optional

from aerospike_client_telemetry.common import series_attributes


@dataclass(frozen=True)
class RecordedExport:
    """Values copied out of one canonical snapshot document."""

    timestamp: str
    client_type: str
    client_version: str
    conn_type: str
    cluster_name: str
    app_id: str
    node_names: tuple[str, ...]
    command_count: Optional[int]
    labels: dict[str, str]


def translate_snapshot(document: Mapping[str, Any]) -> RecordedExport:
    """Copy the fields a test needs out of a canonical snapshot document.

    The document is only read. Nested containers are not stored, so a later
    mutation of the document cannot change the returned record.
    """
    cluster = document.get("cluster") or {}
    command_count = cluster.get("command_count") if isinstance(cluster, Mapping) else None
    nodes = document.get("nodes") or ()
    return RecordedExport(
        timestamp=str(document.get("timestamp", "")),
        client_type=str(document.get("client_type", "")),
        client_version=str(document.get("client_version", "")),
        conn_type=str(document.get("conn_type", "")),
        cluster_name=str(document.get("cluster_name", "")),
        app_id=str(document.get("app_id", "")),
        node_names=tuple(_node_name(node) for node in nodes if isinstance(node, Mapping)),
        command_count=None if command_count is None else int(command_count),
        labels=series_attributes(document),
    )


def _node_name(node: Mapping[str, Any]) -> str:
    name = node.get("name") or ""
    if name:
        return str(name)
    return f"{node.get('address', '')}:{node.get('port', '')}"


class _RecordingState:
    """Callback bookkeeping shared by the sync and async recording exporters."""

    def __init__(self, *, fail_times: int = 0) -> None:
        self._fail_remaining = fail_times
        self._callbacks = 0
        self._errors = 0
        self._records: list[RecordedExport] = []
        self._closed = False
        self._lock = threading.Lock()

    def accept(self, document: Mapping[str, Any]) -> None:
        """Record one translated document, or raise an injected failure."""
        if self._fail_remaining:
            with self._lock:
                self._fail_remaining -= 1
                self._errors += 1
            raise RuntimeError("injected exporter failure")
        record = translate_snapshot(document)
        with self._lock:
            self._records.append(record)
            self._callbacks += 1

    @property
    def callbacks(self) -> int:
        """How many exports completed without error."""
        with self._lock:
            return self._callbacks

    @property
    def errors(self) -> int:
        """How many injected failures have been raised."""
        with self._lock:
            return self._errors

    @property
    def records(self) -> tuple[RecordedExport, ...]:
        """Derived values from each successful export, in arrival order."""
        with self._lock:
            return tuple(self._records)

    @property
    def closed(self) -> bool:
        """Whether application code has closed this exporter."""
        with self._lock:
            return self._closed

    def close(self) -> None:
        """Release exporter-owned resources. The client SDK does not call this."""
        with self._lock:
            self._closed = True


class RecordingExporter(_RecordingState):
    """Sync exporter: plain ``export`` for ``aerospike_sdk.sync.Cluster``."""

    def export(self, snapshot: Any) -> None:
        """Translate one snapshot and return without retaining it."""
        document = snapshot.to_canonical_dict()
        self.accept(document)


class AsyncRecordingExporter(_RecordingState):
    """Async exporter: awaitable ``export`` for ``aerospike_sdk.Cluster``."""

    async def export(self, snapshot: Any) -> None:
        """Translate one snapshot and return without retaining it."""
        document = snapshot.to_canonical_dict()
        self.accept(document)
