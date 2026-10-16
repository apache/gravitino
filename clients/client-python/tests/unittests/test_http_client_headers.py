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
import unittest
from io import BytesIO
from urllib.error import HTTPError
from unittest.mock import Mock, patch

from gravitino.exceptions.base import RESTException
from gravitino.exceptions.handlers.rest_error_handler import REST_ERROR_HANDLER
from gravitino.utils.http_client import HTTPClient


class TestHTTPClientHeaders(unittest.TestCase):
    @staticmethod
    def _success_response():
        response = Mock()
        response.getcode.return_value = 200
        response.read.return_value = b"{}"
        response.info.return_value = {}
        response.url = "http://localhost:8090/test"
        return response

    @staticmethod
    def _capture_requests(mocked_build_opener, responses):
        requests = []
        response_iterator = iter(responses)

        def open_request(request, timeout):
            requests.append(request)
            response = next(response_iterator)
            if isinstance(response, Exception):
                raise response
            return response

        mocked_build_opener.return_value.open.side_effect = open_request
        return requests

    @staticmethod
    def _header_value(request, header_name):
        for name, value in request.header_items():
            if name.lower() == header_name.lower():
                return value
        return None

    def test_request_headers_do_not_leak_between_calls(self):
        configured_headers = {
            "X-Configured": "configured",
            "X-Override": "configured",
        }
        client = HTTPClient("http://localhost:8090", request_headers=configured_headers)

        with patch("gravitino.utils.http_client.build_opener") as build_opener:
            requests = self._capture_requests(
                build_opener, [self._success_response(), self._success_response()]
            )
            client.get(
                "/first",
                headers={"X-Per-Request": "first", "X-Override": "request"},
            )
            client.get("/second")

        self.assertEqual("configured", self._header_value(requests[0], "X-Configured"))
        self.assertEqual("request", self._header_value(requests[0], "X-Override"))
        self.assertEqual("first", self._header_value(requests[0], "X-Per-Request"))
        self.assertEqual("configured", self._header_value(requests[1], "X-Configured"))
        self.assertEqual("configured", self._header_value(requests[1], "X-Override"))
        self.assertIsNone(self._header_value(requests[1], "X-Per-Request"))
        self.assertEqual(
            {"X-Configured": "configured", "X-Override": "configured"},
            configured_headers,
        )
        self.assertEqual(configured_headers, client.request_headers)

    def test_json_and_form_headers_are_request_local(self):
        configured_headers = {
            "X-Configured": "configured",
            "Content-Type": "configured/type",
        }
        client = HTTPClient("http://localhost:8090", request_headers=configured_headers)
        form_data = Mock()
        form_data.to_dict.return_value = {"key": "value"}
        json_data = Mock()
        json_data.to_json.return_value = '{"key":"value"}'

        with patch("gravitino.utils.http_client.build_opener") as build_opener:
            requests = self._capture_requests(
                build_opener, [self._success_response(), self._success_response()]
            )
            client.post("/form", data=form_data)
            client.post("/json", json=json_data)

        self.assertEqual(
            "application/x-www-form-urlencoded",
            self._header_value(requests[0], "Content-Type"),
        )
        self.assertEqual(
            "application/vnd.gravitino.v1+json",
            self._header_value(requests[0], "Accept"),
        )
        self.assertEqual(
            "application/json", self._header_value(requests[1], "Content-Type")
        )
        self.assertEqual(
            "application/vnd.gravitino.v1+json",
            self._header_value(requests[1], "Accept"),
        )
        for request in requests:
            self.assertEqual("configured", self._header_value(request, "X-Configured"))
        self.assertEqual("configured/type", configured_headers["Content-Type"])
        self.assertEqual(configured_headers, client.request_headers)

    def test_failed_request_headers_do_not_leak(self):
        client = HTTPClient("http://localhost:8090")
        error_body = (
            b'{"code":9999,"type":"RESTException",'
            b'"message":"request failed","stack":null}'
        )
        error = HTTPError(
            "http://localhost:8090/failure",
            500,
            "Internal Server Error",
            None,
            BytesIO(error_body),
        )

        with patch("gravitino.utils.http_client.build_opener") as build_opener:
            requests = self._capture_requests(
                build_opener, [error, self._success_response()]
            )
            with self.assertRaises(RESTException):
                client.get(
                    "/failure",
                    headers={"X-Per-Request": "failed"},
                    error_handler=REST_ERROR_HANDLER,
                )
            client.get("/after-failure")

        self.assertEqual("failed", self._header_value(requests[0], "X-Per-Request"))
        self.assertIsNone(self._header_value(requests[1], "X-Per-Request"))
