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

"""Hot-reload of metrics.labels in the SDK configuration file.

A file that sets ``metrics.enabled`` re-applies the whole label map on the
next reload. A file that never sets ``enabled`` leaves the running labels
alone.
"""

from __future__ import annotations

import contextlib
import os
import time

import pytest

from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry import RecordingExporter
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    custom_metrics_settings,
    seed_host,
)

pytestmark = pytest.mark.integration


def _yaml(labels: dict[str, str], *, enabled: bool | None) -> str:
    lines = ["system:", "  DEFAULT:", "    metrics:"]
    if enabled is not None:
        lines.append(f"      enabled: {'true' if enabled else 'false'}")
    lines.append("      labels:")
    lines.extend(f"        {key}: {value}" for key, value in labels.items())
    return "\n".join(lines) + "\n"


def _bump_mtime(path: str) -> None:
    stat = os.stat(path)
    os.utime(path, (stat.st_atime, stat.st_mtime + 2))


@contextlib.contextmanager
def _config_env(path: str):
    previous = os.environ.get("AEROSPIKE_SDK_CONFIG_URL")
    os.environ["AEROSPIKE_SDK_CONFIG_URL"] = path
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("AEROSPIKE_SDK_CONFIG_URL", None)
        else:
            os.environ["AEROSPIKE_SDK_CONFIG_URL"] = previous


def _wait_labels(exporter: RecordingExporter, predicate):
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        records = exporter.records
        if records:
            last = records[-1].labels
            if predicate(last):
                return last
        time.sleep(0.05)
    raise AssertionError(f"labels did not match, last={last}")


def test_a_file_that_owns_metrics_replaces_labels_on_reload(tmp_path):
    path = tmp_path / "sdk.yaml"
    path.write_text(_yaml({"environment": "prod", "owner": "platform"}, enabled=True))
    host, port = seed_host()
    exporter = RecordingExporter()
    with _config_env(str(path)):
        cluster = (
            ClusterDefinition(host, port)
            .app_id("dynamic-labels")
            .with_system_settings(custom_metrics_settings())
            .connect()
        )
        try:
            cluster.add_exporter(exporter)
            assert cluster.metrics_enabled() is True
            first = _wait_labels(
                exporter,
                lambda labels: (
                    labels.get("environment") == "prod" and labels.get("owner") == "platform"
                ),
            )
            assert "another_label" not in first

            path.write_text(_yaml({"environment": "prod", "another_label": "value"}, enabled=True))
            _bump_mtime(str(path))
            reloaded = _wait_labels(
                exporter,
                lambda labels: labels.get("another_label") == "value",
            )
            assert reloaded["environment"] == "prod"
            assert "owner" not in reloaded
        finally:
            cluster.close()
            exporter.close()


def test_a_file_silent_on_enabled_does_not_change_running_labels(tmp_path):
    path = tmp_path / "sdk.yaml"
    path.write_text(_yaml({"environment": "prod"}, enabled=None))
    host, port = seed_host()
    exporter = RecordingExporter()
    with _config_env(str(path)):
        cluster = (
            ClusterDefinition(host, port)
            .app_id("dynamic-labels-silent")
            .with_system_settings(custom_metrics_settings())
            .connect()
        )
        try:
            cluster.add_exporter(exporter)
            cluster.enable_metrics(MetricsPolicy(labels={"environment": "from-code"}))
            _wait_labels(exporter, lambda labels: labels.get("environment") == "from-code")

            path.write_text(_yaml({"environment": "prod", "another_label": "value"}, enabled=None))
            _bump_mtime(str(path))
            time.sleep(2.5)
            latest = exporter.records[-1].labels
            assert latest["environment"] == "from-code"
            assert "another_label" not in latest
        finally:
            cluster.disable_metrics()
            cluster.close()
            exporter.close()
