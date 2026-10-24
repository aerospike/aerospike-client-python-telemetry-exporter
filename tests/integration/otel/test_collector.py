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

"""Aerospike client to exporter to collector to Prometheus.

The webapp is the same shape as ``test_webapp``: one sync cluster, one
``OtelExporter`` that owns the pipeline. The collector receives OTLP/HTTP,
writes a file, and exposes Prometheus text. Prometheus scrapes that endpoint.
The test issues a counted set of inserts, upserts, and each read shape,
then checks those counts against the snapshot latency groups and the
Prometheus counters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import json
import os
from pathlib import Path
import socket
import uuid
import tempfile
import time
from urllib.parse import urlencode
from urllib.request import urlopen

import pytest

from aerospike_sdk import DataSet, Exp
from aerospike_sdk.metrics import MetricsExporterType, MetricsPolicy
from aerospike_sdk.policy.system_settings import MetricsSettings, SystemSettings
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.common import CATALOG
from aerospike_client_telemetry.otel import OtelExporter, OtelExporterConfig, Protocol
from tests.integration.support import CALLBACK_TIMEOUT_SECONDS, namespace, seed_host

pytestmark = pytest.mark.integration

COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.128.0"
PROMETHEUS_IMAGE = "prom/prometheus:v2.55.1"
_DOCKER = Path(__file__).resolve().parents[1] / "docker"
_COLLECTOR_CONFIG = _DOCKER / "otel-collector.yaml"
_PROMETHEUS_CONFIG = _DOCKER / "prometheus.yml"
_COLLECT_TIMEOUT_SECONDS = 25
_INSERTS = 8
_BATCH_SIZE = 4


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def otel_collector():
    """Start the collector and Prometheus. Yield HTTP port, gRPC port, file, and query port."""
    try:
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.network import Network
    except ImportError:
        pytest.skip("testcontainers is not installed; pip install -e '.[dev]'")

    http_port = _free_port()
    grpc_port = _free_port()
    prometheus_port = _free_port()
    output = tempfile.TemporaryDirectory(prefix="otel-collector-")
    os.chmod(output.name, 0o777)
    metrics = Path(output.name) / "metrics.json"
    network = None
    collector = None
    prometheus = None
    try:
        network = Network().create()
        collector = (
            DockerContainer(COLLECTOR_IMAGE)
            .with_network(network)
            .with_network_aliases("otel-collector")
            .with_bind_ports(4318, http_port)
            .with_bind_ports(4317, grpc_port)
            .with_volume_mapping(str(_COLLECTOR_CONFIG), "/etc/otelcol-contrib/config.yaml", "ro")
            .with_volume_mapping(output.name, "/var/otel", "rw")
        )
        collector.start()
        _wait_for_port(http_port)
        _wait_for_port(grpc_port)
        prometheus = (
            DockerContainer(PROMETHEUS_IMAGE)
            .with_network(network)
            .with_bind_ports(9090, prometheus_port)
            .with_volume_mapping(str(_PROMETHEUS_CONFIG), "/etc/prometheus/prometheus.yml", "ro")
        )
        prometheus.start()
        _wait_for_port(prometheus_port)
    except Exception as exc:
        logs = _logs(collector) + "\n" + _logs(prometheus)
        _stop(prometheus, collector, output, network)
        message = f"Unable to start the collector and Prometheus: {exc}\n{logs}"
        if os.environ.get("CI"):
            pytest.fail(message)
        pytest.skip(message)

    try:
        yield http_port, grpc_port, metrics, collector, prometheus_port
    finally:
        _stop(prometheus, collector, output, network)


def _stop(prometheus, collector, output, network) -> None:
    for container in (prometheus, collector):
        if container is not None:
            try:
                container.stop()
            except Exception:
                pass
    if output is not None:
        output.cleanup()
    if network is not None:
        try:
            network.remove()
        except Exception:
            pass


def _wait_for_port(port: int) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"port {port} did not accept connections")


def _logs(container) -> str:
    if container is None:
        return ""
    try:
        stdout, stderr = container.get_logs()
    except Exception:
        return ""
    return stdout.decode(errors="replace") + stderr.decode(errors="replace")


def _wait_for_metric(path: Path, name: str, container) -> str:
    deadline = time.monotonic() + _COLLECT_TIMEOUT_SECONDS
    last = ""
    while time.monotonic() < deadline:
        if path.exists():
            last = path.read_text(errors="replace")
            if name in last:
                return last
        time.sleep(0.2)
    raise AssertionError(
        f"collector file did not contain {name!r}:\n{last[-2000:]}\n{_logs(container)[-2000:]}"
    )


def _query_prometheus(port: int) -> list[dict]:
    query = '{__name__=~"aerospike_client_.+"}'
    url = f"http://127.0.0.1:{port}/api/v1/query?{urlencode({'query': query})}"
    with urlopen(url, timeout=2) as response:
        body = json.loads(response.read().decode())
    if body.get("status") != "success":
        return []
    return body["data"]["result"]


def _format_series(series: list[dict]) -> str:
    lines = []
    for item in sorted(series, key=lambda sample: (sample["metric"].get("__name__", ""), str(sample["metric"]))):
        labels = item["metric"]
        name = labels.get("__name__", "")
        rendered = ",".join(
            f'{key}="{value}"' for key, value in sorted(labels.items()) if key != "__name__"
        )
        lines.append(f"{name}{{{rendered}}} {item['value'][1]}")
    return "\n".join(lines)


@dataclass(frozen=True)
class _Tally:
    """API calls issued after metrics were enabled."""

    inserts: int
    upserts: int
    point_reads: int
    bin_reads: int
    exists_reads: int
    header_reads: int
    batch_reads: int
    batch_writes: int
    scans: int
    filter_queries: int
    list_updates: int

    @property
    def read_calls(self) -> int:
        return self.point_reads + self.bin_reads + self.exists_reads + self.header_reads

    @property
    def write_calls(self) -> int:
        return self.inserts + self.upserts + self.list_updates

    @property
    def batch_calls(self) -> int:
        return self.batch_reads + self.batch_writes

    @property
    def query_calls(self) -> int:
        return self.scans + self.filter_queries

    @property
    def point_calls(self) -> int:
        return self.write_calls + self.read_calls

    @property
    def command_count(self) -> int:
        return self.point_calls + self.batch_calls + self.query_calls

    @property
    def recorded_commands(self) -> int:
        """Calls the SDK command counter includes.

        A single-key ``exists`` is served by the write fast path, which does
        not increment this counter. The read latency histogram still counts it.
        """
        return self.command_count - self.exists_reads

    @property
    def recorded_points(self) -> int:
        return self.point_calls - self.exists_reads


def _drain(stream) -> int:
    seen = 0
    for _ in stream:
        seen += 1
    stream.close()
    return seen


def _run_counted_workload(session, dataset) -> _Tally:
    """Insert, update, and read every shape once for a known number of calls."""
    keys = [dataset.id(i) for i in range(_INSERTS)]
    for index, key in enumerate(keys):
        session.insert(key).put({"sku": f"sku-{index}", "qty": index + 1, "price": 100 + index}).execute()
    for index, key in enumerate(keys):
        session.upsert(key).put({"qty": index + 1, "price": 200 + index}).execute()
    for key in keys:
        session.query(key).execute().first_or_raise()
    for key in keys:
        session.query(key).bins(["sku", "qty"]).execute().first_or_raise()
    for key in keys:
        session.exists(key).execute().first_or_raise()
    for key in keys:
        session.query(key).with_no_bins().execute().first_or_raise()
    batch_reads = 0
    batch_writes = 0
    for start in range(0, _INSERTS, _BATCH_SIZE):
        group = keys[start : start + _BATCH_SIZE]
        session.query(group).execute().collect()
        batch_reads += 1
        # Chained segments. A key list stays on the write fast path and is
        # recorded as a point call, so it would not match a batch count.
        builder = session.upsert(group[0]).put({"qty": 1, "price": 300})
        for key in group[1:]:
            builder = builder.upsert(key).put({"qty": 1, "price": 300})
        builder.execute()
        batch_writes += 1
    for _ in range(2):
        _drain(session.query(dataset).bins(["sku", "qty"]).execute())
    for _ in range(2):
        _drain(
            session.query(dataset)
            .where(Exp.ge(Exp.int_bin("qty"), Exp.int_val(1)))
            .bins(["qty"])
            .execute()
        )
    for key in keys[:4]:
        session.update(key).bin("items").list_append(1).execute()
    return _Tally(
        inserts=_INSERTS,
        upserts=_INSERTS,
        point_reads=_INSERTS,
        bin_reads=_INSERTS,
        exists_reads=_INSERTS,
        header_reads=_INSERTS,
        batch_reads=batch_reads,
        batch_writes=batch_writes,
        scans=2,
        filter_queries=2,
        list_updates=4,
    )


def _command_count(series: list[dict]) -> float:
    for sample in series:
        if sample["metric"].get("__name__") == "aerospike_client_command_count":
            return float(sample["value"][1])
    return 0


def _wait_for_series(port: int, command_count: int) -> list[dict]:
    """Poll until Prometheus reports the calls this test issued."""
    deadline = time.monotonic() + _COLLECT_TIMEOUT_SECONDS
    series: list[dict] = []
    while time.monotonic() < deadline:
        try:
            series = _query_prometheus(port)
        except OSError:
            time.sleep(0.5)
            continue
        if _command_count(series) == command_count:
            return series
        time.sleep(0.5)
    rendered = _format_series(series)
    raise AssertionError(
        f"Prometheus command count is {_command_count(series)}, expected {command_count}:\n{rendered[:2000]}"
    )


def _latency_count(document: dict, group: str, namespace_name: str) -> int:
    total = 0
    for node in document.get("nodes") or []:
        for namespace_doc in node.get("namespaces") or []:
            if namespace_doc.get("name") != namespace_name:
                continue
            buckets = (namespace_doc.get("latency") or {}).get(group) or []
            if isinstance(buckets, list):
                total += sum(buckets)
    return total


def _feature_count(series: list[dict], feature: str) -> float:
    for sample in series:
        labels = sample["metric"]
        if labels.get("__name__") == "aerospike_client_feature_usage" and labels.get("feature") == feature:
            return float(sample["value"][1])
    return 0.0


def _print_coverage(series: list[dict], document: dict, tally: _Tally, namespace_name: str) -> None:
    by_name: dict[str, list[dict]] = {}
    for sample in series:
        by_name.setdefault(sample["metric"].get("__name__", ""), []).append(sample)
    catalog_names = {descriptor.name for descriptor in CATALOG}
    received = set(by_name) & catalog_names
    missing = sorted(catalog_names - received)
    read_seen = _latency_count(document, "read", namespace_name)
    write_seen = _latency_count(document, "write", namespace_name)
    batch_seen = _latency_count(document, "batch", namespace_name)
    query_seen = _latency_count(document, "query", namespace_name)
    lines = [
        "tiers: operational=True usage=True",
        f"full catalog: {len(catalog_names)} metrics",
        f"received: {len(received)} names, {len(series)} series",
        "",
        "calls against counters",
        f"  inserts={tally.inserts} upserts={tally.upserts} list_updates={tally.list_updates}",
        f"  point_reads={tally.point_reads} bin_reads={tally.bin_reads} exists={tally.exists_reads} header_reads={tally.header_reads}",
        f"  batch_reads={tally.batch_reads} batch_writes={tally.batch_writes} scans={tally.scans} filter_queries={tally.filter_queries}",
        f"  command_count {_command_count(series):.0f} expected {tally.recorded_commands}",
        f"  feature.api.blocking {_feature_count(series, 'feature.api.blocking'):.0f} expected {tally.recorded_commands}",
        f"  feature.shape.point {_feature_count(series, 'feature.shape.point'):.0f} expected {tally.recorded_points}",
        f"  feature.shape.batch {_feature_count(series, 'feature.shape.batch'):.0f} expected {tally.batch_calls}",
        f"  feature.shape.query {_feature_count(series, 'feature.shape.query'):.0f} expected {tally.query_calls}",
        f"  feature.cdt {_feature_count(series, 'feature.cdt'):.0f} expected {tally.list_updates}",
        f"  exists calls {tally.exists_reads} are in latency read and not in the command counter",
        f"  latency read {read_seen} expected {tally.read_calls}",
        f"  latency write {write_seen} expected {tally.write_calls}",
        f"  latency batch {batch_seen} expected {tally.batch_calls}",
        f"  latency query {query_seen} expected {tally.query_calls}",
        "",
        "received",
    ]
    for name in sorted(by_name):
        samples = by_name[name]
        if name == "aerospike_client_feature_usage":
            features = ", ".join(
                f"{sample['metric'].get('feature')}={sample['value'][1]}"
                for sample in sorted(samples, key=lambda sample: sample["metric"].get("feature", ""))
            )
            lines.append(f"  {name}  {features}")
            continue
        total = sum(float(sample["value"][1]) for sample in samples)
        lines.append(f"  {name}  {total:g}  series={len(samples)}")
    lines.append("")
    lines.append("not received")
    lines.extend(f"  {name}" for name in missing)
    print("\n".join(lines))


@pytest.mark.parametrize("protocol", [Protocol.HTTP_PROTOBUF, Protocol.GRPC])
def test_webapp_pushes_a_client_metric_to_the_collector(otel_collector, protocol):
    http_port, grpc_port, metrics, container, prometheus_port = otel_collector
    if protocol is Protocol.HTTP_PROTOBUF:
        endpoint = f"http://127.0.0.1:{http_port}/v1/metrics"
    else:
        endpoint = f"http://127.0.0.1:{grpc_port}"
    host, port = seed_host()
    exporter = OtelExporter(
        OtelExporterConfig(
            service_name="orders-service",
            endpoint=endpoint,
            protocol=protocol,
            export_interval=timedelta(seconds=1),
            headers={"authorization": "Bearer orders-token"},
        )
    )
    cluster = None
    try:
        cluster = (
            ClusterDefinition(host, port)
            .app_id("orders-service")
            .with_system_settings(
                SystemSettings(
                    metrics=MetricsSettings(
                        exporter=MetricsExporterType.CUSTOM,
                        export_interval=timedelta(seconds=1),
                    )
                )
            )
            .connect()
        )
    except Exception as exc:
        exporter.close()
        pytest.skip(f"Aerospike seed {host}:{port} unavailable: {exc}")

    try:
        cluster.add_exporter(exporter)
        cluster.enable_metrics(
            MetricsPolicy(
                operational_enabled=True,
                usage_enabled=True,
                labels={"environment": "prod", "region": "us-west"},
            )
        )
        session = cluster.create_session()
        namespace_name = namespace()
        dataset = DataSet.of(namespace_name, f"orders-{uuid.uuid4().hex[:8]}")
        tally = _run_counted_workload(session, dataset)

        deadline = time.monotonic() + CALLBACK_TIMEOUT_SECONDS
        document = None
        seen_count = None
        stable = 0
        while time.monotonic() < deadline:
            current_document = None
            for key in exporter.registry.client_keys():
                current = exporter.registry.current(key)
                if current and current.get("app_id") == "orders-service":
                    current_document = current
                    break
            count = (current_document or {}).get("cluster", {}).get("command_count")
            if current_document is not None and count == seen_count and count:
                stable += 1
                if stable >= 3:
                    document = current_document
                    break
            else:
                stable = 0
                seen_count = count
            time.sleep(0.4)
        assert document is not None, f"command_count did not settle, last value {seen_count}"
        assert document["operational_metrics_enabled"] is True
        assert document["usage_metrics_enabled"] is True

        payload = _wait_for_metric(metrics, "aerospike_client_command_count", container)
        assert "orders-service" in payload
        assert "telemetry-it" in payload
        assert "prod" in payload

        published = int(document["cluster"]["command_count"])
        series = _wait_for_series(prometheus_port, published)
        _print_coverage(series, document, tally, namespace_name)
        command = next(
            sample
            for sample in series
            if sample["metric"].get("__name__") == "aerospike_client_command_count"
        )
        assert command["metric"]["app_id"] == "orders-service"
        assert command["metric"]["cluster"] == "telemetry-it"
        assert command["metric"]["environment"] == "prod"
        assert command["metric"]["service_name"] == "orders-service"
        assert exporter.config.protocol is protocol
        assert float(command["value"][1]) == tally.recorded_commands
        assert _feature_count(series, "feature.api.blocking") == tally.recorded_commands
        assert _feature_count(series, "feature.shape.point") == tally.recorded_points
        assert _feature_count(series, "feature.shape.batch") == tally.batch_calls
        assert _feature_count(series, "feature.shape.query") == tally.query_calls
        assert _feature_count(series, "feature.cdt") == tally.list_updates
        assert _latency_count(document, "read", namespace_name) == tally.read_calls
        assert _latency_count(document, "write", namespace_name) == tally.write_calls
        assert _latency_count(document, "batch", namespace_name) == tally.batch_calls
        assert _latency_count(document, "query", namespace_name) == tally.query_calls

        received = {sample["metric"].get("__name__") for sample in series}
        catalog_names = {descriptor.name for descriptor in CATALOG}
        missing = catalog_names - received
        assert missing == {
            "aerospike_client_namespace_latency_conn_bucket",
            "aerospike_client_namespace_latency_read_bucket",
            "aerospike_client_namespace_latency_write_bucket",
            "aerospike_client_namespace_latency_batch_bucket",
            "aerospike_client_namespace_latency_query_bucket",
        }
        features = {
            sample["metric"].get("feature")
            for sample in series
            if sample["metric"].get("__name__") == "aerospike_client_feature_usage"
        }
        assert "feature.api.blocking" in features
        assert "feature.shape.point" in features
        assert "feature.shape.batch" in features
        assert "feature.shape.query" in features
        assert "feature.cdt" in features
    finally:
        if cluster is not None:
            cluster.disable_metrics()
            cluster.close()
        exporter.close()
