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

"""Counted reads and writes against one live cluster.

This is not a load test. Each case runs a fixed number of writes and reads,
with the default, operational, latency, and feature tiers all enabled, then
checks that the published numbers match that tally.

Run this file on its own::

    python -m pytest tests/integration/otel/test_volume.py

``only_otel`` collects through an OpenTelemetry in-memory reader.
``only_prometheus`` scrapes ``CustomPrometheusMetricReader``.
``otel_and_prometheus`` puts both readers on one meter provider.
"""

from __future__ import annotations

import time
import uuid

import pytest

from aerospike_sdk.dataset import DataSet
from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.support import custom_metrics_settings, namespace, seed_host

pytest.importorskip("opentelemetry.sdk.metrics")
pytest.importorskip("prometheus_client")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from prometheus_client import CollectorRegistry, generate_latest

from aerospike_client_telemetry.otel.prometheus_reader import CustomPrometheusMetricReader

pytestmark = pytest.mark.integration

_WRITES = 1000
_READS = 2000
_COMMANDS = _WRITES + _READS
_SETTLE_SECONDS = 20


def _current(exporter: OtelExporter, app_id: str) -> dict | None:
    for key in exporter.registry.client_keys():
        document = exporter.registry.current(key)
        if document and document.get("app_id") == app_id:
            return document
    return None


def _wait_for_commands(exporter: OtelExporter, app_id: str) -> dict:
    """Wait until the stored snapshot includes every counted call and stops moving."""
    deadline = time.monotonic() + _SETTLE_SECONDS
    seen = None
    stable = 0
    while time.monotonic() < deadline:
        document = _current(exporter, app_id)
        count = None if document is None else document.get("command_count")
        if count == seen and isinstance(count, (int, float)) and count >= _COMMANDS:
            stable += 1
            if stable >= 2:
                return document
        else:
            stable = 0
            seen = count
        time.sleep(0.4)
    raise AssertionError(f"command_count did not settle at {_COMMANDS} or more, last value {seen}")


def _run_workload(cluster) -> str:
    """Insert ``_WRITES`` records, then read ``_READS`` of them back."""
    session = cluster.create_session()
    set_name = f"volume-{uuid.uuid4().hex[:8]}"
    dataset = DataSet.of(namespace(), set_name)
    for index in range(_WRITES):
        session.insert(dataset.id(index)).put({"n": index}).execute()
    for index in range(_READS):
        session.query(dataset.id(index % _WRITES)).execute().first_or_raise()
    return namespace()


def _latency_count(document: dict, group: str, namespace_name: str) -> int:
    total = 0
    for node in document.get("nodes") or []:
        for namespace_doc in node.get("namespaces") or []:
            if namespace_doc.get("namespace") != namespace_name:
                continue
            for latency in namespace_doc.get("latencies") or []:
                if latency.get("type") == group:
                    total += int(latency.get("count", 0))
    return total


def _assert_snapshot(document: dict, namespace_name: str) -> None:
    """The exporter stored every tier of the counted workload."""
    assert document["operational_metrics_enabled"] is True
    assert document["usage_metrics_enabled"] is True
    assert int(document["total_nodes"]) >= 1
    assert "cpu_percent" in document
    assert int(document["command_count"]) == _COMMANDS
    assert _latency_count(document, "write", namespace_name) == _WRITES
    assert _latency_count(document, "read", namespace_name) == _READS
    usage = document["usage"]
    assert int(usage["blocking"]) == _COMMANDS
    assert int(usage["shape_point"]) == _COMMANDS


def _otel_sum(data, name: str, **attributes: str) -> float:
    total = 0.0
    found = False
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name != name:
                    continue
                for point in metric.data.data_points:
                    values = {key: str(value) for key, value in point.attributes.items()}
                    if any(values.get(key) != expected for key, expected in attributes.items()):
                        continue
                    total += float(point.value)
                    found = True
    assert found, f"OpenTelemetry metric {name} {attributes} was not collected"
    return total


def _assert_otel(data, namespace_name: str) -> None:
    assert _otel_sum(data, "aerospike.client.cluster.total_nodes") >= 1
    assert _otel_sum(data, "aerospike.client.cluster.cpu_percent") >= 0
    assert _otel_sum(data, "aerospike.client.cluster.command_count") == _COMMANDS
    assert (
        _otel_sum(
            data,
            "aerospike.client.namespace.latency_bucket",
            latency_type="write",
            le="+Inf",
            ns=namespace_name,
        )
        == _WRITES
    )
    assert (
        _otel_sum(
            data,
            "aerospike.client.namespace.latency_bucket",
            latency_type="read",
            le="+Inf",
            ns=namespace_name,
        )
        == _READS
    )
    assert _otel_sum(data, "aerospike.client.feature.api_blocking") == _COMMANDS
    assert _otel_sum(data, "aerospike.client.feature.shape_point") == _COMMANDS


def _prom_sum(text: str, name: str, **labels: str) -> float:
    total = 0.0
    found = False
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        if not (line.startswith(name + "{") or line.startswith(name + " ")):
            continue
        sample, _, raw = line.rpartition(" ")
        if any(f'{key}="{value}"' not in sample for key, value in labels.items()):
            continue
        total += float(raw)
        found = True
    assert found, f"Prometheus sample {name} {labels} was not scraped"
    return total


def _assert_prometheus(registry: CollectorRegistry, namespace_name: str) -> None:
    text = generate_latest(registry).decode()
    assert _prom_sum(text, "aerospike_client_cluster_total_nodes") >= 1
    assert _prom_sum(text, "aerospike_client_cluster_cpu_percent") >= 0
    assert _prom_sum(text, "aerospike_client_cluster_command_count_total") == _COMMANDS
    assert (
        _prom_sum(
            text,
            "aerospike_client_namespace_latency_bucket_total",
            latency_type="write",
            le="+Inf",
            ns=namespace_name,
        )
        == _WRITES
    )
    assert (
        _prom_sum(
            text,
            "aerospike_client_namespace_latency_bucket_total",
            latency_type="read",
            le="+Inf",
            ns=namespace_name,
        )
        == _READS
    )
    assert _prom_sum(text, "aerospike_client_feature_api_blocking_total") == _COMMANDS
    assert _prom_sum(text, "aerospike_client_feature_shape_point_total") == _COMMANDS


def _connect(app_id: str, exporter: OtelExporter, provider: MeterProvider):
    host, port = seed_host()
    try:
        return (
            ClusterDefinition(host, port)
            .app_id(app_id)
            .with_system_settings(custom_metrics_settings())
            .connect()
        )
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")


def _exercise(provider: MeterProvider, exporter: OtelExporter, app_id: str, check) -> None:
    cluster = _connect(app_id, exporter, provider)
    try:
        cluster.add_exporter(exporter)
        cluster.enable_metrics(MetricsPolicy(operational_enabled=True, usage_enabled=True))
        namespace_name = _run_workload(cluster)
        document = _wait_for_commands(exporter, app_id)
        _assert_snapshot(document, namespace_name)
        check(namespace_name)
    finally:
        try:
            cluster.disable_metrics()
        except Exception:
            pass
        cluster.close()
        provider.shutdown()


def test_only_otel_records_the_counted_workload():
    """An OpenTelemetry reader, and no Prometheus reader, records the tally."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)

    def check(namespace_name: str) -> None:
        _assert_otel(reader.get_metrics_data(), namespace_name)

    _exercise(provider, exporter, "volume-otel", check)


def test_only_prometheus_records_the_counted_workload():
    """A Prometheus scrape, and no OpenTelemetry periodic reader, records the tally."""
    registry = CollectorRegistry()
    reader = CustomPrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[reader])
    exporter = OtelExporter(provider)

    def check(namespace_name: str) -> None:
        _assert_prometheus(registry, namespace_name)

    _exercise(provider, exporter, "volume-prometheus", check)


def test_otel_and_prometheus_record_the_same_counted_workload():
    """Both readers on one provider publish the same tally."""
    otel_reader = InMemoryMetricReader()
    registry = CollectorRegistry()
    prometheus_reader = CustomPrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[otel_reader, prometheus_reader])
    exporter = OtelExporter(provider)

    def check(namespace_name: str) -> None:
        _assert_otel(otel_reader.get_metrics_data(), namespace_name)
        _assert_prometheus(registry, namespace_name)

    _exercise(provider, exporter, "volume-both", check)
