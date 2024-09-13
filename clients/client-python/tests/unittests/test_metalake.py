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

from gravitino.dto.responses.metalake_response import MetalakeResponse


class TestMetalake(unittest.TestCase):
    def test_from_json_metalake_response(self):
        str_json = (
            b'{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment",'
            b'"properties":{"key1":"value1","key2":"value2"},'
            b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        )
        metalake_response = MetalakeResponse.from_json(str_json, infer_missing=True)
        self.assertEqual(metalake_response.code(), 0)
        self.assertIsNotNone(metalake_response.metalake())
        self.assertEqual(metalake_response.metalake().name(), "example_name18")
        self.assertEqual(
            metalake_response.metalake().audit_info().creator(), "anonymous"
        )

    def test_from_error_json_metalake_response(self):
        str_json = (
            b'{"code":0, "undefined-key1":"undefined-value1", '
            b'"metalake":{"undefined-key2":1, "name":"example_name18","comment":"This is a sample comment",'
            b'"properties":{"key1":"value1","key2":"value2"},'
            b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        )
        metalake_response = MetalakeResponse.from_json(str_json, infer_missing=True)
        self.assertEqual(metalake_response.code(), 0)
        self.assertIsNotNone(metalake_response.metalake())
        self.assertEqual(metalake_response.metalake().name(), "example_name18")
        self.assertEqual(
            metalake_response.metalake().audit_info().creator(), "anonymous"
        )
