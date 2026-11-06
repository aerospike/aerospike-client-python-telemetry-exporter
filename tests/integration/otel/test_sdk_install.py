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

"""SDK install check followed by one exporter registration.

The version check is the Python example from the SDK installation guide:
https://aerospike.com/docs/develop/client/sdk/install/
The customer application lifecycle is ``test_webapp.py``.
"""

import time

import pytest

import aerospike_sdk
from aerospike_sdk import DataSet
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.fake_provider import RecordingProvider
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    custom_metrics_settings,
    metrics_policy,
    namespace,
    seed_host,
)

pytestmark = pytest.mark.integration


def test_installed_sdk_pushes_a_snapshot_to_the_exporter():
    # https://aerospike.com/docs/develop/client/sdk/install/
    assert aerospike_sdk.__version__
    assert aerospike_sdk.__version__ != "0.0.0"

    exporter = OtelExporter(RecordingProvider())
    host, port = seed_host()
    definition = (
        ClusterDefinition(host, port)
        .app_id("orders-service")
        .with_system_settings(custom_metrics_settings())
    )
    try:
        cluster = definition.connect()
    except Exception as exc:
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        cluster.enable_metrics(metrics_policy())
        session = cluster.create_session()
        users = DataSet.of(namespace(), "sdk_install")
        session.upsert(users.id(1)).put({"name": "Alice", "age": 28}).execute()

        deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
        document = None
        while time.monotonic() < deadline:
            for key in exporter.registry.client_keys():
                candidate = exporter.registry.current(key)
                if candidate and candidate.get("app_id") == "orders-service":
                    document = candidate
                    break
            if document is not None:
                break
            time.sleep(0.05)
        assert document is not None, "periodic scheduler did not call export()"
        assert document["client_type"] == "python-sdk"
        assert document["client_version"] == aerospike_sdk.__version__
        assert document["app_id"] == "orders-service"
        cluster.disable_metrics()
    finally:
        cluster.close()
