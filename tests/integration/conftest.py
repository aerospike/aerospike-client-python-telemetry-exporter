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

"""Integration fixtures.

One Aerospike server for the session. ``AEROSPIKE_HOST`` selects an existing
server and no container is started. Otherwise an Enterprise 8.2 container is
started on an ephemeral host port and stopped at the end of the session. The
image's built-in single-node evaluation feature key is used.
"""

from __future__ import annotations

import os
from pathlib import Path
import socket
import tempfile
import time

import pytest

# Pin Enterprise Edition. The Python SDK requires server 8.2.0 or later.
# The image includes a single-node evaluation feature key.
SERVER_IMAGE = "aerospike/aerospike-server-enterprise:8.2.0.0"
READY_TIMEOUT_SECONDS = 90
_CONFIG_TEMPLATE = Path(__file__).parent / "docker" / "aerospike.conf"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _render_config(host_port: int, directory: str) -> str:
    text = _CONFIG_TEMPLATE.read_text().replace("__HOST_PORT__", str(host_port))
    path = Path(directory) / "aerospike.conf"
    path.write_text(text)
    return str(path)


def _info(host: str, port: int, command: str) -> str:
    payload = f"{command}\n".encode()
    message = bytes([2, 1]) + len(payload).to_bytes(6, "big") + payload
    with socket.create_connection((host, port), timeout=2) as sock:
        sock.settimeout(2)
        sock.sendall(message)
        data = b""
        while len(data) < 8 or len(data) < 8 + int.from_bytes(data[2:8], "big"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    size = int.from_bytes(data[2:8], "big")
    return data[8 : 8 + size].decode(errors="replace")


def _wait_until_ready(container, host_port: int) -> None:
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last = ""
    while time.monotonic() < deadline:
        stdout, stderr = container.get_logs()
        last = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        if "service ready" in last or "CLUSTER-SIZE 1" in last:
            try:
                build = _info("127.0.0.1", host_port, "build")
            except OSError:
                time.sleep(1)
                continue
            if build.startswith("build\t"):
                return
        if "CRITICAL" in last:
            tail = "\n".join(last.splitlines()[-40:])
            raise RuntimeError(f"Aerospike failed during startup:\n{tail}")
        time.sleep(1)
    tail = "\n".join(last.splitlines()[-40:])
    raise TimeoutError(f"Aerospike did not become ready:\n{tail}")


@pytest.fixture(scope="session", autouse=True)
def aerospike_server():
    """Provide ``AEROSPIKE_HOST`` for every integration test."""
    os.environ.setdefault("AEROSPIKE_NAMESPACE", "test")
    configured = os.environ.get("AEROSPIKE_HOST", "").strip()
    if configured:
        yield configured
        return

    host_port = _free_port()
    try:
        from testcontainers.core.container import DockerContainer
    except ImportError:
        pytest.skip("testcontainers is not installed; pip install -e '.[dev]'")

    directory = tempfile.TemporaryDirectory(prefix="aerospike-telemetry-")
    container = (
        DockerContainer(SERVER_IMAGE)
        .with_bind_ports(3000, host_port)
        .with_volume_mapping(
            _render_config(host_port, directory.name),
            "/etc/aerospike/aerospike.template.conf",
            "ro",
        )
    )
    try:
        container.start()
        _wait_until_ready(container, host_port)
    except Exception as exc:
        container.stop()
        directory.cleanup()
        message = f"Unable to start Aerospike {SERVER_IMAGE}: {exc}"
        if os.environ.get("CI"):
            pytest.fail(message)
        pytest.skip(message)

    seed = f"127.0.0.1:{host_port}"
    # access-address and access-port are already the host endpoint. Alternate
    # discovery is empty on this single node and the client times out with it on.
    os.environ["AEROSPIKE_HOST"] = seed
    os.environ["AEROSPIKE_USE_SERVICES_ALTERNATE"] = "false"
    try:
        yield seed
    finally:
        container.stop()
        directory.cleanup()
