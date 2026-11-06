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

"""Throwaway TLS material for Prometheus scrape tests.

Certificates are written into a temporary directory by the caller. They are
not stored in this repository. ``.gitignore`` ignores ``*.pem``, ``*.crt``,
``*.csr``, ``*.key``, and ``*.srl`` so a local copy cannot be committed.
"""

from __future__ import annotations

import shutil
import ssl
import subprocess
from pathlib import Path

import pytest

needs_openssl = pytest.mark.skipif(
    shutil.which("openssl") is None,
    reason="openssl is required to mint test certificates",
)


def mint_certificates(root: Path) -> None:
    """Write a CA, server certificate, and client certificate under ``root``."""
    _openssl(
        root, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", "ca.key", "-out", "ca.crt", "-subj", "/CN=test-ca", "-days", "1",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-addext", "subjectKeyIdentifier=hash",
    )
    _openssl(
        root, "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost",
    )
    _openssl(
        root, "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
        "-CAcreateserial", "-out", "server.crt", "-days", "1",
    )
    _openssl(
        root, "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", "client.key", "-out", "client.csr", "-subj", "/CN=scraper",
    )
    _openssl(
        root, "x509", "-req", "-in", "client.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
        "-CAcreateserial", "-out", "client.crt", "-days", "1",
    )


def tls_client(client_cert: Path | None = None) -> ssl.SSLContext:
    """HTTPS client. Pass ``client_cert`` when the server requires mutual TLS."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    if client_cert is not None:
        context.load_cert_chain(str(client_cert), str(client_cert.with_suffix(".key")))
    return context


def _openssl(directory: Path, *args: str) -> None:
    subprocess.run(
        ["openssl", *args],
        cwd=directory,
        check=True,
        capture_output=True,
        text=True,
    )
