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

"""Shared setup for periodic-export integration tests."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.metrics import MetricsExporterType, MetricsPolicy
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings

# The first scheduler tick waits one full interval. One second keeps the
# acceptance test on the real timer without a long wait.
EXPORT_INTERVAL_SECONDS = 1
CALLBACK_TIMEOUT_SECONDS = 8

os.environ.setdefault("AEROSPIKE_NAMESPACE", "test")


def seed_host() -> tuple[str, int]:
    """Return the configured Aerospike seed as ``(host, port)``."""
    raw = os.environ["AEROSPIKE_HOST"]
    host, separator, port = raw.rpartition(":")
    if not separator or not host or not port.isdigit():
        raise ValueError(f"AEROSPIKE_HOST must be host:port, got {raw!r}")
    return host, int(port)


def custom_metrics_settings() -> SystemSettings:
    """CUSTOM mode so registered exporters, and only those, receive the push."""
    return SystemSettings(
        metrics=MetricsSettings(
            exporter=MetricsExporterType.CUSTOM,
            export_interval=timedelta(seconds=EXPORT_INTERVAL_SECONDS),
        )
    )


def metrics_policy() -> MetricsPolicy:
    """Operational tier so generated traffic is visible on the snapshot."""
    return MetricsPolicy(operational_enabled=True)


def namespace() -> str:
    return os.environ["AEROSPIKE_NAMESPACE"]


def dataset():
    return DataSet.of(namespace(), "telemetry_exporter")


def assert_same_cycle(left, right) -> None:
    """Two exporters observed one scheduler cycle.

    ``to_canonical_dict()`` stamps ``timestamp`` when it is called, so two
    translations of the same snapshot differ by the time between the calls.
    A gap well under the export interval is the same cycle.
    """
    assert left.node_names == right.node_names
    assert left.command_count == right.command_count
    assert left.app_id == right.app_id
    left_at = datetime.fromisoformat(left.timestamp)
    right_at = datetime.fromisoformat(right.timestamp)
    assert abs(left_at - right_at) < timedelta(milliseconds=500)
