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
from unittest.mock import Mock, patch

from gravitino.utils.http_client import HTTPClient


class TestHTTPClientRawBody(unittest.TestCase):
    @patch("gravitino.utils.http_client.build_opener")
    def test_raw_text_and_bytes_use_existing_auth_and_timeout(self, build_opener):
        auth = Mock()
        auth.get_token_data.return_value = b"Bearer token"
        client = HTTPClient("http://localhost", auth_data_provider=auth)
        for body in ("name: café 雪\n", b"name: sales\n", "", b""):
            with self.subTest(body=body):
                client.post(
                    "ossie",
                    data=body,
                    headers={"Content-Type": "application/yaml"},
                    timeout=7,
                )
                request = build_opener.return_value.open.call_args.args[0]
                self.assertEqual(
                    body.encode("utf-8") if isinstance(body, str) else body,
                    request.data,
                )
                self.assertEqual("application/yaml", request.get_header("Content-type"))
                self.assertEqual("Bearer token", request.get_header("Authorization"))
                self.assertEqual(
                    7, build_opener.return_value.open.call_args.kwargs["timeout"]
                )

    @patch("gravitino.utils.http_client.build_opener")
    def test_existing_form_and_json_bodies(self, build_opener):
        client = HTTPClient("http://localhost")
        body = Mock()
        body.to_dict.return_value = {"grant_type": "client_credentials", "scope": "a b"}
        client.post_form("token", data=body)
        request = build_opener.return_value.open.call_args.args[0]
        self.assertEqual(b"grant_type=client_credentials&scope=a+b", request.data)
        self.assertEqual(
            "application/x-www-form-urlencoded", request.get_header("Content-type")
        )
        body.to_json.return_value = '{"name":"café"}'
        client.post("models", json=body)
        request = build_opener.return_value.open.call_args.args[0]
        self.assertEqual('{"name":"café"}'.encode("utf-8"), request.data)
        self.assertEqual("application/json", request.get_header("Content-type"))
