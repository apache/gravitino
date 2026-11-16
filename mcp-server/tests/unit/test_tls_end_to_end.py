# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""End-to-end TLS with a private CA, over real connections.

Covers both hops: the MCP endpoint served over HTTPS with --tls-cert/--tls-key,
and outbound calls to a Gravitino server whose certificate is signed by a
private CA that is trusted through SSL_CERT_FILE.
"""

import datetime
import http.server
import ipaddress
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import certifi
import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)

_PROJECT_DIR = Path(__file__).resolve().parents[2]
_METALAKES = {"code": 0, "metalakes": [{"name": "lake_over_tls"}]}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _write_private_ca(directory: Path) -> tuple[Path, Path, Path]:
    """Write a private CA and a localhost certificate it signs.

    Returns the CA certificate, the server certificate and the server key.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    server_key = ec.generate_private_key(ec.SECP256R1())
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        )
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = directory / "ca.crt"
    cert_path = directory / "server.crt"
    key_path = directory / "server.key"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca_path, cert_path, key_path


class _GravitinoHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # pylint: disable=invalid-name
        body = json.dumps(_METALAKES).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # Keep the test output quiet.
        pass


class _PrivateCaFixture:
    """Certificates from one private CA, and a fake Gravitino served over TLS."""

    def __init__(self):
        self.directory = Path(tempfile.mkdtemp())
        self.ca, self.cert, self.key = _write_private_ca(self.directory)
        # SSL_CERT_FILE replaces the default roots, so the bundle carries both.
        self.bundle = self.directory / "bundle.crt"
        self.bundle.write_bytes(
            Path(certifi.where()).read_bytes() + self.ca.read_bytes()
        )
        self.gravitino = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), _GravitinoHandler
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert, self.key)
        self.gravitino.socket = context.wrap_socket(
            self.gravitino.socket, server_side=True
        )
        self.gravitino_uri = f"https://localhost:{self.gravitino.server_port}"
        threading.Thread(
            target=self.gravitino.serve_forever, daemon=True
        ).start()

    def close(self):
        self.gravitino.shutdown()
        self.gravitino.server_close()
        shutil.rmtree(self.directory, ignore_errors=True)


class TestGravitinoOverTls(unittest.IsolatedAsyncioTestCase):
    """Outbound calls to a Gravitino server whose certificate a private CA signs."""

    @classmethod
    def setUpClass(cls):
        cls.tls = _PrivateCaFixture()

    @classmethod
    def tearDownClass(cls):
        cls.tls.close()

    async def _list_metalakes(self, trust: dict) -> str:
        # httpx reads SSL_CERT_FILE when the client is created.
        with patch.dict(os.environ, trust):
            operation = PlainRESTClientOperation("lake", self.tls.gravitino_uri)
        try:
            return (
                await operation.as_metalake_operation().get_list_of_metalakes()
            )
        finally:
            await operation.close()

    async def test_gravitino_signed_by_trusted_private_ca(self):
        result = await self._list_metalakes(
            {"SSL_CERT_FILE": str(self.tls.bundle), "SSL_CERT_DIR": ""}
        )
        self.assertIn("lake_over_tls", result)

    async def test_gravitino_signed_by_untrusted_private_ca(self):
        with self.assertRaisesRegex(
            httpx.ConnectError, "CERTIFICATE_VERIFY_FAILED"
        ):
            await self._list_metalakes(
                {"SSL_CERT_FILE": "", "SSL_CERT_DIR": ""}
            )


class TestMcpEndpointOverTls(unittest.TestCase):
    """The MCP endpoint served with --tls-cert/--tls-key, from a real process."""

    @classmethod
    def setUpClass(cls):
        cls.tls = _PrivateCaFixture()

    @classmethod
    def tearDownClass(cls):
        cls.tls.close()

    def test_mcp_endpoint_served_over_tls(self):
        port = _free_port()
        url = f"https://127.0.0.1:{port}/mcp"
        process = subprocess.Popen(  # pylint: disable=consider-using-with
            [
                sys.executable,
                "-m",
                "mcp_server",
                "--metalake",
                "lake",
                "--gravitino-uri",
                self.tls.gravitino_uri,
                "--transport",
                "streamable-http",
                "--mcp-url",
                url,
                "--tls-cert",
                str(self.tls.cert),
                "--tls-key",
                str(self.tls.key),
            ],
            cwd=_PROJECT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            trusted = ssl.create_default_context(cafile=str(self.tls.ca))
            response = self._wait_for_tls(url, trusted, process)
            # The endpoint answers over TLS; a plain GET is not a valid MCP request.
            self.assertLess(response.status_code, 500)
            with self.assertRaisesRegex(
                httpx.ConnectError, "CERTIFICATE_VERIFY_FAILED"
            ):
                httpx.get(
                    url,
                    verify=ssl.create_default_context(cafile=certifi.where()),
                )
            with self.assertRaises(httpx.HTTPError):
                httpx.get(url.replace("https://", "http://", 1))
        finally:
            process.terminate()
            process.wait(timeout=30)

    @staticmethod
    def _wait_for_tls(url, context, process, timeout=60.0):
        deadline = time.monotonic() + timeout
        while True:
            try:
                return httpx.get(url, verify=context)
            except httpx.ConnectError:
                if process.poll() is not None:
                    raise AssertionError(
                        f"MCP server exited with {process.returncode}"
                    ) from None
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)


if __name__ == "__main__":
    unittest.main()
