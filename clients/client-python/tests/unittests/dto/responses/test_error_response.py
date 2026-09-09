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

from gravitino.dto.responses.error_response import ErrorResponse


class TestErrorResponse(unittest.TestCase):
    def test_format_error_message_without_stack(self):
        response = ErrorResponse.from_json(
            '{"code": 1000, "type": "RESTException", "message": "mock error"}',
            infer_missing=True,
        )

        self.assertIsNone(response.stack())
        self.assertEqual("mock error", response.format_error_message())

    def test_format_error_message_with_empty_stack(self):
        response = ErrorResponse(1000, "RESTException", "mock error", [])

        self.assertEqual("mock error", response.format_error_message())

    def test_format_error_message_with_stack(self):
        response = ErrorResponse.from_json(
            '{"code": 1000, "type": "RESTException", "message": "mock error", '
            '"stack": ["stack line 1", "stack line 2"]}'
        )

        self.assertEqual(["stack line 1", "stack line 2"], response.stack())
        self.assertEqual(
            "mock error\nstack line 1\nstack line 2", response.format_error_message()
        )
