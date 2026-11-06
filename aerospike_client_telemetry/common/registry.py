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

"""Latest canonical document for each client the process exports.

An OpenTelemetry observable instrument reads this registry when the OTel SDK
collects, so the Aerospike export thread only replaces the current document
and does not perform exporter network I/O. The registry stores the document
mapping it is given; callers pass a document they are finished with.
"""

from __future__ import annotations

from collections.abc import Mapping
import threading
from typing import Any, Optional


class ClientSnapshotRegistry:
    """Thread-safe map of client key to the latest canonical document."""

    def __init__(self) -> None:
        self._documents: dict[str, Mapping[str, Any]] = {}
        self._lock = threading.Lock()

    def replace(self, client_key: str, document: Mapping[str, Any]) -> None:
        """Publish ``document`` as the current snapshot for ``client_key``."""
        with self._lock:
            self._documents[client_key] = document

    def current(self, client_key: str) -> Optional[Mapping[str, Any]]:
        """Return the latest document for ``client_key``, or ``None``."""
        with self._lock:
            return self._documents.get(client_key)

    def client_keys(self) -> tuple[str, ...]:
        """Client keys that currently have a document, in insertion order."""
        with self._lock:
            return tuple(self._documents)
