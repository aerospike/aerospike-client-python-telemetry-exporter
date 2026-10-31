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

"""One Compose project for the integration containers.

Docker Desktop groups containers that share ``com.docker.compose.project``.
The names follow Compose: ``<project>-<service>-1`` on the network
``<project>_default``.
"""

from __future__ import annotations

from pathlib import Path

PROJECT = "aerospike-python-telemetry"
NETWORK_NAME = f"{PROJECT}_default"
_REPO = Path(__file__).resolve().parents[2]
_network = None


def container_name(service: str) -> str:
    """Compose-style container name for one service."""
    return f"{PROJECT}-{service}-1"


def labels(service: str) -> dict[str, str]:
    """Labels that place a container in the Compose project."""
    return {
        "com.docker.compose.project": PROJECT,
        "com.docker.compose.service": service,
        "com.docker.compose.container-number": "1",
        "com.docker.compose.oneoff": "False",
        "com.docker.compose.project.working_dir": str(_REPO),
        "com.docker.compose.project.config_files": "tests/integration/docker",
    }


def project_network():
    """The shared network, created once per session."""
    global _network
    if _network is not None:
        return _network
    from testcontainers.core.network import Network

    _drop_stale_project()
    network = Network(
        docker_network_kw={
            "labels": {
                "com.docker.compose.project": PROJECT,
                "com.docker.compose.network": "default",
                "com.docker.compose.version": "2",
            }
        }
    )
    network.name = NETWORK_NAME
    network.create()
    _network = network
    return network


def stop_network() -> None:
    """Remove the project network after its containers are gone."""
    global _network
    network = _network
    _network = None
    if network is None:
        return
    try:
        network.remove()
    except Exception:
        pass


def _drop_stale_project() -> None:
    """Drop a project left behind by a crashed run so the names can be reused."""
    for service in (
        "prometheus",
        "otel-collector",
        "aerospike-two",
        "aerospike",
        "aerospike-down",
        "aerospike-pair-a",
        "aerospike-pair-b",
    ):
        _drop_container(container_name(service))
    _drop_network(NETWORK_NAME)


def _client():
    from testcontainers.core.docker_client import DockerClient

    return DockerClient().client


def _drop_container(name: str) -> None:
    try:
        _client().containers.get(name).remove(force=True)
    except Exception:
        pass


def _drop_network(name: str) -> None:
    try:
        _client().networks.get(name).remove()
    except Exception:
        pass
