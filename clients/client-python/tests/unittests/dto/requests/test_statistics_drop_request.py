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

from gravitino.dto.requests.statistics_drop_request import StatisticsDropRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestStatisticsDropRequest(unittest.TestCase):
    def test_validate_success(self):
        names = ["custom-statistic1", "custom-statistic2"]
        request = StatisticsDropRequest(_names=names)
        self.assertListEqual(request.names, names)
        request.validate()

    def test_validate_failure_names(self):
        request = StatisticsDropRequest(_names=[])
        with self.assertRaises(IllegalArgumentException) as context:
            request.validate()
        self.assertIn('"names" must not be null or empty', str(context.exception))

        request = StatisticsDropRequest(_names=None)
        with self.assertRaises(IllegalArgumentException) as context:
            request.validate()
        self.assertIn('"names" must not be null or empty', str(context.exception))

    def test_validate_failure_empty_name_in_list(self):
        request = StatisticsDropRequest(_names=["statistic1", "", "statistic2"])
        with self.assertRaises(IllegalArgumentException) as context:
            request.validate()
        self.assertIn("Each name must be a non-empty string", str(context.exception))

    def test_statistics_drop_request_serialize(self):
        names = ["custom-statistic1", "custom-statistic2"]
        request = StatisticsDropRequest(_names=names)
        expected_json = json.dumps({"names": names})
        self.assertEqual(expected_json, request.to_json())

    def test_statistics_drop_request_deserialize(self):
        names = ["custom-statistic1", "custom-statistic2"]
        json_str = json.dumps({"names": names})
        request = StatisticsDropRequest.from_json(json_str)
        self.assertListEqual(names, request.names)
