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

import threading
import traceback
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from gravitino.exceptions.base import HTTPResponseException, UnauthorizedException
from gravitino.exceptions.handlers.oauth_error_handler import OAUTH_ERROR_HANDLER
from gravitino.exceptions.handlers.rest_error_handler import REST_ERROR_HANDLER
from gravitino.utils.http_client import HTTPClient


class _ErrorServer(BaseHTTPRequestHandler):
    def do_GET(self):  # pylint: disable=invalid-name
        """Serve deliberately malformed responses and valid domain errors."""
        if self.path == "/success":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
            return
        payloads = {
            "/html": b"<html>private-response</html>",
            "/empty": b"",
            "/text": b"private-response",
            "/gravitino": b'{"code":1011,"type":"UnauthorizedException","message":"denied","stack":null}',
            "/oauth": b'{"error":"invalid_client"}',
        }
        self.send_response(401 if self.path in ("/oauth", "/gravitino") else 502)
        # Valid JSON must work even when an intermediary mislabels its content type.
        self.send_header("Content-Type", "text/html")
        if self.path in ("/truncated", "/timeout"):
            self.send_header("Content-Length", "100")
        self.end_headers()
        if self.path == "/timeout":
            self.wfile.flush()
            self.server.release.wait(5)
        else:
            self.wfile.write(payloads.get(self.path, b"private-partial"))

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        """Keep fixture request logging out of test output."""


class TestHTTPClientErrors(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ErrorServer)
        self.server.release = threading.Event()
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.addCleanup(self._stop_server)
        self.environment = patch.dict(
            "os.environ", {"no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"}
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"
        self.client = HTTPClient(self.origin)

    def _stop_server(self):
        self.server.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        self.assertFalse(self.thread.is_alive())

    def test_gateway_responses_and_recovery(self):
        for endpoint in ("html", "empty", "text", "truncated", "timeout"):
            with self.subTest(endpoint=endpoint):
                self.server.release.clear()
                with self.assertRaises(HTTPResponseException) as raised:
                    self.client.get(
                        endpoint, timeout=0.2, error_handler=REST_ERROR_HANDLER
                    )
                self.assertEqual(502, raised.exception.status_code)
                self.assertEqual("GET", raised.exception.request_method)
                self.assertEqual(self.origin, raised.exception.endpoint)
                self.assertNotIn(
                    "private-", "".join(traceback.format_exception(raised.exception))
                )
                if endpoint in ("truncated", "timeout"):
                    self.assertIn("Unable to read", str(raised.exception))
                self.server.release.set()
                self.assertEqual({"ok": True}, self.client.get("success").json())

    def test_domain_errors_with_mislabeled_content_type(self):
        for endpoint, handler in (
            ("gravitino", REST_ERROR_HANDLER),
            ("oauth", OAUTH_ERROR_HANDLER),
        ):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(UnauthorizedException):
                    self.client.get(endpoint, error_handler=handler)
