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
import unittest

from gravitino.dto.responses.file_location_response import FileLocationResponse
from gravitino.exceptions.base import IllegalArgumentException


class TestResponses(unittest.TestCase):
    def test_file_location_response(self):
        json_data = {"code": 0, "fileLocation": "file:/test/1"}
        json_str = json.dumps(json_data)
        file_location_resp: FileLocationResponse = FileLocationResponse.from_json(
            json_str
        )
        self.assertEqual(file_location_resp.file_location(), "file:/test/1")
        file_location_resp.validate()

    def test_file_location_response_exception(self):
        json_data = {"code": 0, "fileLocation": ""}
        json_str = json.dumps(json_data)
        file_location_resp: FileLocationResponse = FileLocationResponse.from_json(
            json_str
        )
        with self.assertRaises(IllegalArgumentException):
            file_location_resp.validate()
