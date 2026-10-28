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

"""A webapp meter provider across enable and disable.

``OtelExporter(provider)`` registers instruments on that provider.
It does not start a reader and it does not open OTLP. The webapp records its
own instruments on the same provider. ``enable_metrics`` and
``disable_metrics`` start and stop the Aerospike export thread. The provider
keeps reporting the last client document while that thread is stopped, and
the webapp instruments keep recording.
"""

from __future__ import annotations

import threading
import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.support import (
    CALLBACK_TIMEOUT_SECONDS,
    EXPORT_INTERVAL_SECONDS,
    custom_metrics_settings,
    metrics_policy,
    namespace,
    seed_host,
)

pytestmark = pytest.mark.integration

_EXPORT_THREAD_NAME = "aerospike-metrics-export"


def _reject_otlp(self, *args, **kwargs):
    raise AssertionError("a passed-in meter provider opened an OTLP connection")


def _live_export_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name == _EXPORT_THREAD_NAME and thread.is_alive()
    ]


def _metric_value(reader: InMemoryMetricReader, name: str) -> float | None:
    data = reader.get_metrics_data()
    if data is None:
        return None
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name != name:
                    continue
                points = list(metric.data.data_points)
                if not points:
                    return None
                return float(points[0].value)
    return None


def _wait_for_value(reader: InMemoryMetricReader, predicate) -> float:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        last = _metric_value(reader, "aerospike.client.cluster.command_count")
        if last is not None and predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"aerospike.client.cluster.command_count is {last}")


def _write(cluster, dataset) -> None:
    session = cluster.create_session()
    for i in range(3):
        session.upsert(dataset.id(i)).put({"n": i}).execute()


def _running_provider(monkeypatch):
    http = pytest.importorskip("opentelemetry.exporter.otlp.proto.http.metric_exporter")
    grpc = pytest.importorskip("opentelemetry.exporter.otlp.proto.grpc.metric_exporter")
    monkeypatch.setattr(http.OTLPMetricExporter, "__init__", _reject_otlp)
    monkeypatch.setattr(grpc.OTLPMetricExporter, "__init__", _reject_otlp)
    reader = InMemoryMetricReader()
    return reader, MeterProvider(metric_readers=[reader])


def _webapp_instruments(provider):
    """Instruments the webapp owns on the same provider as the client exporter."""
    orders = provider.get_meter("orders")
    return orders.create_counter("orders_placed"), orders.create_gauge("orders_backlog")


def _collected(reader: InMemoryMetricReader) -> dict[str, tuple[str, float]]:
    data = reader.get_metrics_data()
    found: dict[str, tuple[str, float]] = {}
    if data is None:
        return found
    for resource in data.resource_metrics:
        for scope_metrics in resource.scope_metrics:
            scope_name = scope_metrics.scope.name
            for metric in scope_metrics.metrics:
                points = list(metric.data.data_points)
                if points:
                    found[metric.name] = (scope_name, float(points[0].value))
    return found


def _wait_for_client_count(reader: InMemoryMetricReader, predicate) -> dict[str, tuple[str, float]]:
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        collected = _collected(reader)
        client = collected.get("aerospike.client.cluster.command_count")
        last = None if client is None else client[1]
        if last is not None and predicate(last):
            return collected
        time.sleep(0.05)
    raise AssertionError(f"aerospike.client.cluster.command_count is {last}")


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


def test_passed_meter_provider_survives_enable_and_disable(monkeypatch):
    reader, provider = _running_provider(monkeypatch)
    exporter = OtelExporter(provider)
    assert exporter.meter_provider is provider
    assert list(provider._metric_readers) == [reader]

    cluster = _connect("passed-provider", exporter, provider)

    dataset = DataSet.of(namespace(), "passed_provider")
    try:
        cluster.add_exporter(exporter)
        already_running = {thread.ident for thread in _live_export_threads()}
        cluster.enable_metrics(metrics_policy())
        started = [
            thread for thread in _live_export_threads() if thread.ident not in already_running
        ]
        assert len(started) == 1
        _write(cluster, dataset)
        first = _wait_for_value(reader, lambda value: value >= 1)

        cluster.disable_metrics()
        assert cluster.metrics_enabled() is False
        assert started[0].is_alive() is False
        held = _metric_value(reader, "aerospike.client.cluster.command_count")
        assert held == first
        _write(cluster, dataset)
        time.sleep(EXPORT_INTERVAL_SECONDS * 2.5)
        assert _metric_value(reader, "aerospike.client.cluster.command_count") == held
        assert provider._shutdown is False

        cluster.enable_metrics(metrics_policy())
        assert cluster.metrics_enabled() is True
        _write(cluster, dataset)
        second = _wait_for_value(reader, lambda value: value > held)
        cluster.disable_metrics()
        assert _metric_value(reader, "aerospike.client.cluster.command_count") == second
        cluster.close()
        cluster = None

        assert provider._shutdown is False
        assert _metric_value(reader, "aerospike.client.cluster.command_count") == second
    finally:
        if cluster is not None:
            cluster.close()
        provider.shutdown()


def test_webapp_metrics_and_client_metrics_share_the_provider(monkeypatch):
    reader, provider = _running_provider(monkeypatch)
    placed, backlog = _webapp_instruments(provider)
    exporter = OtelExporter(provider)
    cluster = _connect("shared-provider", exporter, provider)
    dataset = DataSet.of(namespace(), "shared_provider")
    try:
        placed.add(2)
        backlog.set(4)
        cluster.add_exporter(exporter)
        cluster.enable_metrics(metrics_policy())
        _write(cluster, dataset)
        collected = _wait_for_client_count(reader, lambda value: value >= 1)
        assert collected["orders_placed"] == ("orders", 2)
        assert collected["orders_backlog"] == ("orders", 4)
        scope, value = collected["aerospike.client.cluster.command_count"]
        assert scope == "aerospike.client"
        assert value >= 1
    finally:
        cluster.disable_metrics()
        cluster.close()
        provider.shutdown()


def test_webapp_metrics_continue_while_client_metrics_are_disabled(monkeypatch):
    reader, provider = _running_provider(monkeypatch)
    placed, backlog = _webapp_instruments(provider)
    exporter = OtelExporter(provider)
    cluster = _connect("webapp-while-disabled", exporter, provider)
    dataset = DataSet.of(namespace(), "webapp_while_disabled")
    try:
        placed.add(2)
        backlog.set(4)
        cluster.add_exporter(exporter)
        cluster.enable_metrics(metrics_policy())
        _write(cluster, dataset)
        collected = _wait_for_client_count(reader, lambda value: value >= 1)
        client = collected["aerospike.client.cluster.command_count"][1]
        assert collected["orders_placed"] == ("orders", 2)
        assert collected["orders_backlog"] == ("orders", 4)

        cluster.disable_metrics()
        assert cluster.metrics_enabled() is False
        placed.add(3)
        backlog.set(9)
        _write(cluster, dataset)
        time.sleep(EXPORT_INTERVAL_SECONDS * 2.5)
        held = _collected(reader)
        assert held["aerospike.client.cluster.command_count"] == ("aerospike.client", client)
        assert held["orders_placed"] == ("orders", 5)
        assert held["orders_backlog"] == ("orders", 9)

        cluster.enable_metrics(metrics_policy())
        placed.add(1)
        _write(cluster, dataset)
        resumed = _wait_for_client_count(reader, lambda value: value > client)
        assert resumed["orders_placed"] == ("orders", 6)
        assert resumed["orders_backlog"] == ("orders", 9)
        assert resumed["aerospike.client.cluster.command_count"][0] == "aerospike.client"
    finally:
        cluster.disable_metrics()
        cluster.close()
        assert provider._shutdown is False
        provider.shutdown()


def _wait_for_snapshot(exporter: OtelExporter, predicate):
    deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
    last = None
    while time.monotonic() < deadline:
        for key in exporter.registry.client_keys():
            document = exporter.registry.current(key)
            if document and predicate(document):
                return document
            last = document
        time.sleep(0.05)
    raise AssertionError(f"snapshot did not match, last flags {last}")


def test_operational_and_usage_flags_gate_the_callback(monkeypatch):
    reader, provider = _running_provider(monkeypatch)
    exporter = OtelExporter(provider)
    cluster = _connect("tier-toggle", exporter, provider)
    dataset = DataSet.of(namespace(), "tier_toggle")
    try:
        cluster.add_exporter(exporter)
        cluster.enable_metrics(MetricsPolicy())
        assert cluster.metrics_enabled() is True
        _write(cluster, dataset)
        gauges_only = _wait_for_snapshot(
            exporter,
            lambda document: document.get("metrics_enabled") is True
            and document.get("operational_metrics_enabled") is False
            and document.get("usage_metrics_enabled") is False,
        )
        collected = _collected(reader)
        assert "aerospike.client.cluster.total_nodes" in collected
        assert "aerospike.client.cluster.command_count" not in collected
        assert "aerospike.client.feature.api_blocking" not in collected

        cluster.enable_metrics(MetricsPolicy(operational_enabled=True))
        _write(cluster, dataset)
        operational = _wait_for_snapshot(
            exporter,
            lambda document: document.get("operational_metrics_enabled") is True
            and document.get("usage_metrics_enabled") is False
            and document.get("timestamp") != gauges_only["timestamp"]
            and document.get("command_count", 0) >= 1,
        )
        collected = _collected(reader)
        assert collected["aerospike.client.cluster.command_count"][0] == "aerospike.client"
        assert "aerospike.client.feature.api_blocking" not in collected
        assert "aerospike.client.cluster.total_nodes" in collected

        cluster.enable_metrics(MetricsPolicy(operational_enabled=True, usage_enabled=True))
        _write(cluster, dataset)
        both = _wait_for_snapshot(
            exporter,
            lambda document: document.get("operational_metrics_enabled") is True
            and document.get("usage_metrics_enabled") is True
            and document.get("timestamp") != operational["timestamp"]
            and document.get("usage", {}).get("blocking", 0) >= 1,
        )
        collected = _collected(reader)
        assert "aerospike.client.cluster.command_count" in collected
        assert collected["aerospike.client.feature.api_blocking"][0] == "aerospike.client"

        cluster.enable_metrics(MetricsPolicy(usage_enabled=True))
        _wait_for_snapshot(
            exporter,
            lambda document: document.get("operational_metrics_enabled") is False
            and document.get("usage_metrics_enabled") is True
            and document.get("timestamp") != both["timestamp"],
        )
        collected = _collected(reader)
        assert "aerospike.client.cluster.command_count" not in collected
        assert "aerospike.client.feature.api_blocking" in collected
        assert "aerospike.client.cluster.total_nodes" in collected

        cluster.enable_metrics(MetricsPolicy())
        _wait_for_snapshot(
            exporter,
            lambda document: document.get("operational_metrics_enabled") is False
            and document.get("usage_metrics_enabled") is False
            and document.get("metrics_enabled") is True,
        )
        collected = _collected(reader)
        assert cluster.metrics_enabled() is True
        assert "aerospike.client.cluster.command_count" not in collected
        assert "aerospike.client.feature.api_blocking" not in collected
        assert "aerospike.client.cluster.total_nodes" in collected
    finally:
        cluster.disable_metrics()
        cluster.close()
        provider.shutdown()
