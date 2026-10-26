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

"""The exporter registers instruments on a meter provider it does not own."""

import pytest

from aerospike_client_telemetry.common import CATALOG
from aerospike_client_telemetry.otel import OtelExporter
from tests.fake_provider import RecordingProvider


def test_a_meter_provider_is_required():
    with pytest.raises(TypeError, match="meter_provider"):
        OtelExporter()


def test_instruments_are_registered_and_close_leaves_the_provider_running():
    provider = RecordingProvider()
    exporter = OtelExporter(provider)
    try:
        assert exporter.meter_provider is provider
        assert provider.meter_name == "aerospike.client"
        registered = set(provider.meter.counters) | set(provider.meter.gauges)
        assert registered == {descriptor.name for descriptor in CATALOG}
    finally:
        exporter.close()
    assert provider.shutdowns == 0
