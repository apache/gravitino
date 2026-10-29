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

import json
import traceback
import unittest
from http.client import IncompleteRead
from io import BytesIO
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from gravitino.exceptions.base import (
    BadRequestException,
    HTTPResponseException,
    RESTException,
    UnauthorizedException,
)
from gravitino.exceptions.handlers.error_handler import ErrorHandler
from gravitino.exceptions.handlers.oauth_error_handler import OAUTH_ERROR_HANDLER
from gravitino.exceptions.handlers.rest_error_handler import REST_ERROR_HANDLER
from gravitino.utils.http_client import HTTPClient


class TestHTTPClientErrors(unittest.TestCase):
    @staticmethod
    def _error(body):
        return HTTPError(
            "https://user:password@example.com/private?secret=value",
            502,
            "private-reason",
            {"Set-Cookie": "private-cookie"},
            BytesIO(body),
        )

    def _request(self, error, handler=REST_ERROR_HANDLER, method="get", host=None):
        with patch("gravitino.utils.http_client.build_opener") as opener:
            opener.return_value.open.side_effect = error
            client = HTTPClient(host or "https://user:password@example.com:8443")
            return getattr(client, method)(
                "private-path?secret=value#fragment",
                headers={"Authorization": "Bearer private-token"},
                error_handler=handler,
            )

    def test_invalid_responses_preserve_safe_context(self):
        payloads = [
            b"",
            b"  ",
            b"<html>private-body</html>",
            b"private-body",
            b"{",
            b"\xff",
            b"null",
            b"[]",
            b'"text"',
            b"42",
            b"{}",
            b'{"code":1011}',
            b'{"code":true,"type":"X","message":"X"}',
            b'{"code":-1,"type":"X","message":"X"}',
            b'{"code":"1011","type":"X","message":"X"}',
            b'{"code":1011,"type":1,"message":"X"}',
            b'{"code":1011,"type":"X","message":{}}',
            b'{"code":1011,"type":"X","message":"X","stack":[1]}',
            b'{"error":1}',
            b'{"error":"invalid_client","error_description":[]}',
            b'{"error":"unknown","stack":true}',
            b"private-body" * 10000,
        ]
        for payload in payloads:
            with self.subTest(payload=payload[:60]):
                error = self._error(payload)
                with self.assertRaises(HTTPResponseException) as raised:
                    self._request(error)
                exception = raised.exception
                self.assertIsInstance(exception, RESTException)
                self.assertEqual(502, exception.status_code)
                self.assertEqual("GET", exception.request_method)
                self.assertEqual("https://example.com:8443", exception.endpoint)
                diagnostic = "".join(traceback.format_exception(exception))
                for private in (
                    "password",
                    "private-path",
                    "secret=value",
                    "fragment",
                    "private-token",
                    "private-body",
                    "private-reason",
                    "private-cookie",
                    "JSONDecodeError",
                    "HTTPError:",
                ):
                    self.assertNotIn(private, diagnostic)
                self.assertTrue(error.closed)

    def test_read_failures_keep_http_status(self):
        for failure in (
            IncompleteRead(b"private-partial", 20),
            OSError("private"),
            TimeoutError("private"),
        ):
            with self.subTest(failure=type(failure).__name__):
                error = self._error(b"")
                error.read = Mock(side_effect=failure)
                with self.assertRaises(HTTPResponseException) as raised:
                    self._request(error, method="post")
                self.assertEqual(502, raised.exception.status_code)
                self.assertEqual("POST", raised.exception.request_method)
                self.assertIn("Unable to read", str(raised.exception))
                self.assertNotIn(
                    "private", "".join(traceback.format_exception(raised.exception))
                )
                self.assertTrue(error.closed)

    def test_close_failure_does_not_replace_http_failure(self):
        error = self._error(b"not-json")
        original_close = error.close
        error.close = Mock(side_effect=OSError("private-close-error"))
        try:
            with self.assertRaises(HTTPResponseException):
                self._request(error)
        finally:
            original_close()

    def test_transport_failures_are_not_converted(self):
        for failure in (
            URLError("unreachable"),
            ConnectionError("disconnected"),
            TimeoutError("timeout"),
        ):
            with self.subTest(failure=type(failure).__name__):
                with self.assertRaises(type(failure)) as raised:
                    self._request(failure)
                self.assertIs(failure, raised.exception)

    def test_gravitino_errors_keep_mapping_and_unknown_fields(self):
        for code, expected in ((1011, UnauthorizedException), (1999, RESTException)):
            body = json.dumps(
                {
                    "code": code,
                    "type": "FutureException",
                    "message": "failure",
                    "stack": ["server stack"],
                    "future": True,
                }
            ).encode()
            with self.subTest(code=code):
                with self.assertRaises(expected) as raised:
                    self._request(self._error(body))
                self.assertNotIsInstance(raised.exception, HTTPResponseException)
                self.assertIn("failure\nserver stack", str(raised.exception))

    def test_oauth2_errors_keep_mapping_and_optional_description(self):
        for error, expected in (
            ("invalid_client", UnauthorizedException),
            ("invalid_grant", BadRequestException),
            ("future_error", RESTException),
        ):
            for description in (None, "failure"):
                body = {"error": error, "future": True}
                if description is not None:
                    body["error_description"] = description
                with self.subTest(error=error, description=description):
                    with self.assertRaises(expected) as raised:
                        self._request(
                            self._error(json.dumps(body).encode()), OAUTH_ERROR_HANDLER
                        )
                    self.assertNotIsInstance(raised.exception, HTTPResponseException)

    def test_handler_exception_is_not_converted(self):
        failure = ValueError("custom handler failure")
        handler = Mock(spec=ErrorHandler)
        handler.handle.side_effect = failure
        body = (
            b'{"code":1999,"type":"FutureException","message":"failure","stack":null}'
        )
        with self.assertRaises(ValueError) as raised:
            self._request(self._error(body), handler)
        self.assertIs(failure, raised.exception)
        handler.handle.assert_called_once()

    def test_fallback_with_no_or_custom_handler(self):
        handler = Mock(spec=ErrorHandler)
        for value in (None, handler):
            with self.subTest(handler=value):
                with self.assertRaises(HTTPResponseException):
                    self._request(self._error(b""), value)
        handler.handle.assert_not_called()

    def test_safe_endpoints(self):
        for host, origin in (
            ("http://[::1]:8090", "http://[::1]:8090"),
            ("https://user:secret@example.com", "https://example.com"),
            ("https://example.com:invalid", "<unknown endpoint>"),
        ):
            with self.subTest(host=host):
                with self.assertRaises(HTTPResponseException) as raised:
                    self._request(self._error(b""), host=host)
                self.assertEqual(origin, raised.exception.endpoint)
