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

from gravitino.api.stats.json_serdes.statistic_value_serdes import StatisticValueSerdes
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.dto.requests.statistics_update_request import StatisticsUpdateRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestStatisticsUpdateRequest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.test_updates = {
            "custom-long-statistic": StatisticValues.long_value(100),
            "custom-str-statistic": StatisticValues.string_value("value"),
            "custom-list-statistic": StatisticValues.list_value(
                [StatisticValues.double_value(1.5), StatisticValues.double_value(2.5)]
            ),
            "custom-object-statistic": StatisticValues.object_value(
                {
                    "key1": StatisticValues.boolean_value(True),
                    "key2": StatisticValues.long_value(42),
                }
            ),
        }

    def test_validate_success(self):
        request = StatisticsUpdateRequest(_updates=self.test_updates)
        self.assertDictEqual(request.updates, self.test_updates)
        request.validate()

    def test_validate_failure_empty_name(self):
        updates = {"": StatisticValues.long_value(100)}
        request = StatisticsUpdateRequest(_updates=updates)
        with self.assertRaisesRegex(
            IllegalArgumentException, 'statistic "name" must not be null or empty'
        ):
            request.validate()

    def test_validate_failure_null_value(self):
        updates = {"custom-null-value": None}
        request = StatisticsUpdateRequest(_updates=updates)
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "statistic \"value\" for 'custom-null-value' must not be null",
        ):
            request.validate()

    def test_statistics_update_request_serialize(self):
        request = StatisticsUpdateRequest(_updates=self.test_updates)
        expected_json = json.dumps(
            {
                "updates": {
                    key: StatisticValueSerdes.serialize(value)
                    for key, value in self.test_updates.items()
                }
            }
        )
        self.assertEqual(request.to_json(), expected_json)

    def test_statistics_update_request_deserialize(self):
        json_str = json.dumps(
            {
                "updates": {
                    key: StatisticValueSerdes.serialize(value)
                    for key, value in self.test_updates.items()
                }
            }
        )
        request = StatisticsUpdateRequest.from_json(json_str)
        self.assertDictEqual(request.updates, self.test_updates)

    def test_validate_failure_null_or_empty_updates(self):
        invalid_updates = (None, {})
        for updates in invalid_updates:
            request = StatisticsUpdateRequest(_updates=updates)
            with self.assertRaisesRegex(
                IllegalArgumentException, '"updates" must not be null or empty'
            ):
                request.validate()
