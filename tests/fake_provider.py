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

"""A meter provider that records instrument registration and does not export."""


class RecordingMeter:
    """Stores the observable instruments an exporter registers."""

    def __init__(self) -> None:
        self.gauges: dict[str, list] = {}
        self.counters: dict[str, list] = {}

    def create_observable_gauge(self, name, callbacks=None, unit="", description=""):
        self.gauges[name] = list(callbacks or [])

    def create_observable_counter(self, name, callbacks=None, unit="", description=""):
        self.counters[name] = list(callbacks or [])


class RecordingProvider:
    """Enough of a meter provider for tests that only read the registry."""

    def __init__(self) -> None:
        self.meter = RecordingMeter()
        self.shutdowns = 0

    def get_meter(self, name, version=None, schema_url=None):
        self.meter_name = name
        return self.meter

    def shutdown(self) -> None:
        self.shutdowns += 1
