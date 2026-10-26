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

"""Hot-reload of the operational tier in the SDK configuration file.

The file owns ``metrics.enabled``. Turning ``extended.operational.enabled``
from true to false keeps collection on and the next callback omits the
operational series.
"""

from __future__ import annotations

import contextlib
import os

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.otel.test_passed_provider import (
    _collected,
    _running_provider,
    _wait_for_snapshot,
    _write,
)
from tests.integration.support import custom_metrics_settings, namespace, seed_host

pytestmark = pytest.mark.integration


def _yaml(*, operational: bool) -> str:
    flag = "true" if operational else "false"
    return (
        "system:\n"
        "  DEFAULT:\n"
        "    metrics:\n"
        "      enabled: true\n"
        "      extended:\n"
        "        operational:\n"
        f"          enabled: {flag}\n"
    )


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


def test_a_file_reload_drops_operational_metrics(tmp_path, monkeypatch):
    path = tmp_path / "sdk.yaml"
    path.write_text(_yaml(operational=True))
    host, port = seed_host()
    reader, provider = _running_provider(monkeypatch)
    exporter = OtelExporter(provider)
    with _config_env(str(path)):
        cluster = (
            ClusterDefinition(host, port)
            .app_id("dynamic-operational")
            .with_system_settings(custom_metrics_settings())
            .connect()
        )
        try:
            cluster.add_exporter(exporter)
            assert cluster.metrics_enabled() is True
            dataset = DataSet.of(namespace(), "dynamic_operational")
            _write(cluster, dataset)
            enabled = _wait_for_snapshot(
                exporter,
                lambda document: (
                    document.get("metrics_enabled") is True
                    and document.get("operational_metrics_enabled") is True
                    and document.get("command_count", 0) >= 1
                ),
            )
            collected = _collected(reader)
            assert collected["aerospike_client_command_count"][0] == "aerospike.client"
            assert "aerospike_client_total_nodes" in collected

            path.write_text(_yaml(operational=False))
            _bump_mtime(str(path))
            _wait_for_snapshot(
                exporter,
                lambda document: (
                    document.get("metrics_enabled") is True
                    and document.get("operational_metrics_enabled") is False
                    and document.get("timestamp") != enabled["timestamp"]
                ),
            )
            assert cluster.metrics_enabled() is True
            collected = _collected(reader)
            assert "aerospike_client_command_count" not in collected
            assert "aerospike_client_total_nodes" in collected
        finally:
            cluster.close()
            exporter.close()
            provider.shutdown()
