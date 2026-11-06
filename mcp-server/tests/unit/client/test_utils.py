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

"""Regression tests for precision in shared REST response extraction."""

import json
import unittest
from decimal import Decimal

import httpx

from mcp_server.client.plain.utils import (
    extract_content_from_response,
    extract_response,
)


class TestResponseExtraction(unittest.TestCase):
    def test_extractors_preserve_nested_numbers(self):
        response = httpx.Response(
            200,
            content=(
                b'{"code":0,"data":{"nested":['
                b"0.12345678901234567890123456789,"
                b"1234567890123456789012345678901234567890,"
                b'1.00,true,null,"extra"]}}'
            ),
        )
        expected = {
            "nested": [
                Decimal("0.12345678901234567890123456789"),
                1234567890123456789012345678901234567890,
                Decimal("1.00"),
                True,
                None,
                "extra",
            ]
        }
        self.assertEqual(
            expected,
            json.loads(
                extract_content_from_response(response, "data"),
                parse_float=Decimal,
            ),
        )
        self.assertEqual(
            {"code": 0, "data": expected},
            json.loads(extract_response(response), parse_float=Decimal),
        )
