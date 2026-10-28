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

"""What keeps flowing when a server stops and metrics stay enabled.

The export thread does not stop with the database. These tests stop a
container the client was using and read the next snapshots, plus the series
the callback still publishes from them.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import time

import pytest

from aerospike_sdk import DataSet
from aerospike_sdk.metrics import MetricsPolicy
from aerospike_sdk.sync import ClusterDefinition

from aerospike_client_telemetry.otel import OtelExporter
from tests.integration.compose_group import container_name, labels, project_network
from tests.integration.conftest import (
    READY_TIMEOUT_SECONDS,
    _free_port,
    _info,
    _start_server,
)
from tests.integration.otel.test_passed_provider import _collected, _running_provider, _write
from tests.integration.support import custom_metrics_settings, namespace

pytestmark = pytest.mark.integration

COMMUNITY_IMAGE = "aerospike/aerospike-server:8.2.0.0"
_PAIR_CONFIG = Path(__file__).parents[1] / "docker" / "aerospike-pair.conf"
_OUTAGE_SECONDS = 20
_CLUSTER_FORM_SECONDS = 40


def _brief(document: dict) -> dict:
    nodes = document.get("nodes") or []
    departed = document.get("nodes_departed") or []

    def _host(node: dict) -> str:
        return f"{node.get('node_address')}:{node.get('node_port')}"

    return {
        "timestamp": document.get("timestamp"),
        "metrics_enabled": document.get("metrics_enabled"),
        "operational_metrics_enabled": document.get("operational_metrics_enabled"),
        "usage_metrics_enabled": document.get("usage_metrics_enabled"),
        "total_nodes": document.get("total_nodes"),
        "invalid_node_count": document.get("invalid_node_count"),
        "open_connections": document.get("open_connections"),
        "recover_queue_size": document.get("recover_queue_size"),
        "command_count": document.get("command_count"),
        "command_retries": document.get("command_retries"),
        "nodes": [_host(node) for node in nodes],
        "nodes_departed": [_host(node) for node in departed],
    }


def _latest(exporter: OtelExporter) -> dict | None:
    for key in exporter.registry.client_keys():
        document = exporter.registry.current(key)
        if document:
            return document
    return None


def _watch(exporter: OtelExporter, seconds: float, predicate):
    """Return the first new-enough snapshot that matches, and every one seen."""
    history = []
    seen: set[str] = set()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        document = _latest(exporter)
        if document and document.get("timestamp") not in seen:
            seen.add(document["timestamp"])
            history.append(_brief(document))
            if predicate(document):
                return document, history
        time.sleep(0.1)
    return None, history


def _connect(seed: str, app_id: str, exporter: OtelExporter, provider, *, alternate: bool):
    host, _, port = seed.rpartition(":")
    try:
        return (
            ClusterDefinition(host, int(port))
            .app_id(app_id)
            .using_services_alternate(alternate)
            .with_system_settings(custom_metrics_settings())
            .connect()
        )
    except Exception as exc:
        provider.shutdown()
        pytest.skip(f"Aerospike seed {seed} unavailable: {exc}")


def _published(reader) -> tuple[set[str], set[str]]:
    """Client metric names present on this collect, and the node labels among them."""
    data = reader.get_metrics_data()
    names: set[str] = set()
    nodes: set[str] = set()
    if data is None:
        return names, nodes
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            if scope.scope.name != "aerospike.client":
                continue
            for metric in scope.metrics:
                points = list(metric.data.data_points)
                if not points:
                    continue
                names.add(metric.name)
                for point in points:
                    node = dict(point.attributes).get("server_node_id")
                    if node:
                        nodes.add(str(node))
    return names, nodes


def _enable(cluster, exporter: OtelExporter, dataset) -> dict:
    cluster.add_exporter(exporter)
    cluster.enable_metrics(MetricsPolicy(operational_enabled=True, usage_enabled=True))
    assert cluster.metrics_enabled() is True
    _write(cluster, dataset)
    document, history = _watch(
        exporter,
        _OUTAGE_SECONDS,
        lambda doc: (
            doc.get("metrics_enabled") is True
            and doc.get("operational_metrics_enabled") is True
            and doc.get("usage_metrics_enabled") is True
            and int(doc.get("total_nodes") or 0) >= 1
            and doc.get("nodes")
        ),
    )
    assert document is not None, f"no healthy snapshot: {history}"
    return document


def test_metrics_keep_flowing_when_the_only_server_stops(monkeypatch):
    container, directory, seed = _start_server("telemetry-down", "aerospike-down")
    reader, provider = _running_provider(monkeypatch)
    exporter = OtelExporter(provider)
    cluster = _connect(seed, "server-down", exporter, provider, alternate=False)
    dataset = DataSet.of(namespace(), "server_down")
    try:
        healthy = _enable(cluster, exporter, dataset)
        assert len(healthy["nodes"]) == 1
        before = int(healthy["command_count"])
        live_host = f"{healthy['nodes'][0]['node_address']}:{healthy['nodes'][0]['node_port']}"

        container.stop()
        # The only seed stays in the client's node list while tend is refused.
        # Nothing remains to announce a departure, so the next snapshots still
        # describe that node and the callback keeps publishing it.
        _, history = _watch(exporter, 8, lambda _document: False)
        after = [row for row in history if row["timestamp"] != healthy["timestamp"]]
        assert len(after) >= 2, f"exports did not continue after the stop: {history}"
        for row in after:
            assert row["metrics_enabled"] is True
            assert row["operational_metrics_enabled"] is True
            assert row["usage_metrics_enabled"] is True
            assert row["total_nodes"] == 1
            assert row["invalid_node_count"] == 0
            assert row["nodes"] == [live_host]
            assert row["nodes_departed"] == []
            assert row["command_count"] == before

        names, node_labels = _published(reader)
        assert "aerospike.client.cluster.total_nodes" in names
        assert "aerospike.client.cluster.invalid_node_count" in names
        assert "aerospike.client.cluster.open_connections" in names
        assert "aerospike.client.cluster.recover_queue_size" in names
        assert "aerospike.client.cluster.command_count" in names
        assert "aerospike.client.cluster.cpu_percent" in names
        assert "aerospike.client.cluster.memory_bytes" in names
        assert "aerospike.client.feature.api_blocking" in names
        assert "aerospike.client.node.connections_in_pool" in names
        assert len(node_labels) == 1
        collected = _collected(reader)
        assert collected["aerospike.client.cluster.total_nodes"] == ("aerospike.client", 1)
        assert collected["aerospike.client.cluster.invalid_node_count"] == ("aerospike.client", 0)
        assert collected["aerospike.client.cluster.command_count"][1] == before
    finally:
        try:
            cluster.disable_metrics()
        except Exception:
            pass
        try:
            cluster.close()
        except Exception:
            pass
        provider.shutdown()
        try:
            container.stop()
        except Exception:
            pass
        directory.cleanup()


def _render_pair(host_port: int, directory: str) -> str:
    text = (
        _PAIR_CONFIG.read_text()
        .replace("__HOST_PORT__", str(host_port))
        .replace("__CLUSTER_NAME__", "telemetry-pair")
    )
    path = Path(directory) / "aerospike.conf"
    path.write_text(text)
    return str(path)


def _start_pair_node(service: str):
    from testcontainers.core.container import DockerContainer

    host_port = _free_port()
    directory = tempfile.TemporaryDirectory(prefix=f"{service}-")
    container = (
        DockerContainer(COMMUNITY_IMAGE)
        .with_name(container_name(service))
        .with_kwargs(labels=labels(service))
        .with_network(project_network())
        .with_network_aliases(service)
        .with_bind_ports(3000, host_port)
        .with_volume_mapping(
            _render_pair(host_port, directory.name),
            "/etc/aerospike/aerospike.template.conf",
            "ro",
        )
    )
    try:
        container.start()
        _wait_until_listening(host_port)
    except Exception as exc:
        tail = _log_tail(container)
        try:
            container.stop()
        except Exception:
            pass
        directory.cleanup()
        raise RuntimeError(f"{exc}\n{tail}") from exc
    return container, directory, f"127.0.0.1:{host_port}"


def _wait_until_listening(host_port: int) -> None:
    """Wait until info answers. Mesh peers log errors until the other node is up."""
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            build = _info("127.0.0.1", host_port, "build")
        except OSError:
            time.sleep(1)
            continue
        if build.startswith("build\t"):
            return
        time.sleep(1)
    raise TimeoutError(f"Aerospike on port {host_port} did not answer info")


def _log_tail(container) -> str:
    try:
        stdout, stderr = container.get_logs()
    except Exception as exc:
        return f"(no logs: {exc})"
    text = stdout.decode(errors="replace") + stderr.decode(errors="replace")
    return "\n".join(text.splitlines()[-40:])


def _cluster_size(seed: str) -> int | None:
    host, _, port = seed.rpartition(":")
    try:
        stats = _info(host, int(port), "statistics")
    except OSError:
        return None
    for part in stats.replace("statistics\t", "").split(";"):
        if part.startswith("cluster_size="):
            return int(part.split("=", 1)[1])
    return None


def _wait_for_cluster_size(seeds: list[str], size: int, started) -> None:
    deadline = time.monotonic() + _CLUSTER_FORM_SECONDS
    last = None
    while time.monotonic() < deadline:
        last = [_cluster_size(seed) for seed in seeds]
        if len(last) == len(seeds) and all(value == size for value in last):
            return
        time.sleep(1)
    tails = "\n---\n".join(_log_tail(item[0]) for item in started)
    raise AssertionError(f"cluster size did not become {size}: {last}\n{tails}")


def _stop_pair(*started) -> None:
    for item in started:
        container, directory = item[0], item[1]
        try:
            container.stop()
        except Exception:
            pass
        directory.cleanup()


def test_one_node_of_a_two_node_cluster_drops_out_of_the_callback(monkeypatch):
    try:
        from testcontainers.core.container import DockerContainer  # noqa: F401
    except ImportError:
        pytest.skip("testcontainers is not installed; pip install -e '.[dev]'")

    started = []
    try:
        for service in ("aerospike-pair-a", "aerospike-pair-b"):
            started.append(_start_pair_node(service))
    except Exception as exc:
        _stop_pair(*started)
        pytest.fail(f"Unable to start Aerospike {COMMUNITY_IMAGE}: {exc}")

    left, right = started
    seeds = [left[2], right[2]]
    reader, provider = _running_provider(monkeypatch)
    exporter = OtelExporter(provider)
    cluster = None
    try:
        try:
            _wait_for_cluster_size(seeds, 2, started)
        except AssertionError:
            for container, _directory, seed in started:
                container._container.restart(timeout=30)
                _wait_until_listening(int(seed.rpartition(":")[2]))
            _wait_for_cluster_size(seeds, 2, started)

        cluster = _connect(seeds[0], "pair-down", exporter, provider, alternate=True)
        dataset = DataSet.of(namespace(), "pair_down")
        healthy = _enable(cluster, exporter, dataset)
        assert int(healthy["total_nodes"]) == 2
        assert len(healthy["nodes"]) == 2
        before = int(healthy["command_count"])
        stopped_host = f"{healthy['nodes'][1]['node_address']}:{healthy['nodes'][1]['node_port']}"
        # The snapshot addresses are the alternate host endpoints. Stop the
        # container whose published port is that endpoint.
        victim = left if left[2] == stopped_host else right
        survivor = right if victim is left else left

        victim[0].stop()
        down, history = _watch(
            exporter,
            _OUTAGE_SECONDS,
            lambda doc: (
                doc.get("timestamp") != healthy["timestamp"]
                and len(doc.get("nodes") or []) == 1
                and any(
                    stopped_host == f"{node.get('node_address')}:{node.get('node_port')}"
                    for node in doc.get("nodes_departed") or []
                )
            ),
        )
        assert down is not None, f"node stop was not reflected: {history}"
        assert down["metrics_enabled"] is True
        assert down["operational_metrics_enabled"] is True
        assert int(down["total_nodes"]) == 1
        assert int(down["command_count"]) >= before
        survivor_host = f"{down['nodes'][0]['node_address']}:{down['nodes'][0]['node_port']}"
        assert survivor_host == survivor[2]

        names, node_labels = _published(reader)
        assert "aerospike.client.cluster.total_nodes" in names
        assert "aerospike.client.cluster.invalid_node_count" in names
        assert "aerospike.client.cluster.command_count" in names
        assert "aerospike.client.node.connections_in_pool" in names
        assert _collected(reader)["aerospike.client.cluster.total_nodes"][1] == 1
        assert len(node_labels) == 1
    finally:
        if cluster is not None:
            try:
                cluster.disable_metrics()
            except Exception:
                pass
            try:
                cluster.close()
            except Exception:
                pass
        provider.shutdown()
        _stop_pair(*started)
