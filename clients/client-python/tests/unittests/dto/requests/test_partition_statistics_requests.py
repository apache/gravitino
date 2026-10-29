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

from gravitino.api.stats.partition_statistics_drop import PartitionStatisticsDrop
from gravitino.api.stats.partition_statistics_update import PartitionStatisticsUpdate
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.dto.requests.partition_statistics_drop_request import (
    PartitionStatisticsDropRequest,
)
from gravitino.dto.requests.partition_statistics_update_request import (
    PartitionStatisticsUpdateRequest,
)
from gravitino.exceptions.base import IllegalArgumentException


class TestPartitionStatisticsRequests(unittest.TestCase):
    def test_update_wire_format_and_round_trip(self):
        values = {
            "custom-false": StatisticValues.boolean_value(False),
            "custom-long": StatisticValues.long_value(9223372036854775807),
            "custom-double": StatisticValues.double_value(0.0),
            "custom-string": StatisticValues.string_value(""),
            "custom-list": StatisticValues.list_value([StatisticValues.long_value(0)]),
            "custom-object": StatisticValues.object_value(
                {"x": StatisticValues.boolean_value(False)}
            ),
        }
        update = PartitionStatisticsUpdate("dt=2026-10-29/地区=甲", values)
        request = PartitionStatisticsUpdateRequest([update, update])
        request.validate()
        expected = {
            "partitionName": update.partition_name,
            "statistics": {
                "custom-false": False,
                "custom-long": 9223372036854775807,
                "custom-double": 0.0,
                "custom-string": "",
                "custom-list": [0],
                "custom-object": {"x": False},
            },
        }
        self.assertEqual(
            {"updates": [expected, expected]}, json.loads(request.to_json())
        )
        decoded = PartitionStatisticsUpdateRequest.from_json(request.to_json())
        decoded.validate()
        self.assertEqual(request, decoded)

    def test_drop_wire_format_and_round_trip(self):
        drop = PartitionStatisticsDrop("p1", ["custom-b", "custom-a", "custom-b"])
        request = PartitionStatisticsDropRequest([drop, drop])
        request.validate()
        self.assertEqual(
            {
                "drops": [
                    {"partitionName": "p1", "statisticNames": drop.statistic_names}
                ]
                * 2
            },
            json.loads(request.to_json()),
        )
        decoded = PartitionStatisticsDropRequest.from_json(request.to_json())
        decoded.validate()
        self.assertEqual(request, decoded)

    def test_invalid_batch_envelopes(self):
        for cls in [PartitionStatisticsUpdateRequest, PartitionStatisticsDropRequest]:
            for items in [None, [], [None], ["p1"]]:
                with self.subTest(cls=cls, items=items), self.assertRaises(
                    IllegalArgumentException
                ):
                    cls(items).validate()

    def test_mutated_inputs_are_revalidated_at_request_boundary(self):
        values = {"custom-x": StatisticValues.long_value(1)}
        update = PartitionStatisticsUpdate("p1", values)
        values.clear()
        with self.assertRaises(IllegalArgumentException):
            PartitionStatisticsUpdateRequest([update]).validate()
        names = ["custom-x"]
        drop = PartitionStatisticsDrop("p1", names)
        names.append(" ")
        with self.assertRaises(IllegalArgumentException):
            PartitionStatisticsDropRequest([drop]).validate()
