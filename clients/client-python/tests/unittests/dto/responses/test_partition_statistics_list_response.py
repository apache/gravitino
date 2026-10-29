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

import copy
import json
import unittest
import warnings

from gravitino.api.stats.partition_statistics import PartitionStatistics
from gravitino.dto.responses.partition_statistics_list_response import (
    PartitionStatisticsListResponse,
)
from gravitino.exceptions.base import IllegalArgumentException


class TestPartitionStatisticsListResponse(unittest.TestCase):
    def setUp(self):
        self.statistic = {
            "name": "custom-x",
            "reserved": False,
            "modifiable": True,
            "audit": {"creator": "tester"},
            "value": {"nested": [False], "zero": 0, "empty": ""},
        }
        self.payload = {
            "code": 0,
            "partitionStatistics": [
                {"partitionName": "p1", "statistics": [self.statistic]}
            ],
        }

    def decode(self, payload):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            result = PartitionStatisticsListResponse.from_json(
                json.dumps(payload), infer_missing=True
            )
        result.validate()
        return result

    def test_nested_results_and_audit_round_trip(self):
        result = self.decode(self.payload)
        partition = result.partition_statistics[0]
        self.assertIsInstance(partition, PartitionStatistics)
        self.assertEqual("p1", partition.partition_name())
        statistic = partition.statistics()[0]
        self.assertFalse(statistic.reserved())
        self.assertTrue(statistic.modifiable())
        self.assertEqual("tester", statistic.audit_info().creator())
        self.assertEqual(False, statistic.value().value()["nested"].value()[0].value())
        self.assertEqual(result, self.decode(json.loads(result.to_json())))

    def test_empty_results_and_empty_statistics_are_valid(self):
        self.assertEqual(
            [], self.decode({"code": 0, "partitionStatistics": []}).partition_statistics
        )
        self.payload["partitionStatistics"][0]["statistics"] = []
        self.assertEqual(
            [], self.decode(self.payload).partition_statistics[0].statistics()
        )

    def test_unset_optional_statistic_value_is_valid(self):
        del self.statistic["value"]
        self.assertIsNone(
            self.decode(self.payload).partition_statistics[0].statistics()[0].value()
        )

    def test_missing_or_null_required_result_fields(self):
        payloads = [
            {"code": 0},
            {"partitionStatistics": []},
            {"code": -1, "partitionStatistics": []},
            {"code": 0, "partitionStatistics": None},
            {"code": 0, "partitionStatistics": [None]},
            {"code": 0, "partitionStatistics": [{"statistics": []}]},
            {"code": 0, "partitionStatistics": [{"partitionName": "p1"}]},
            {
                "code": 0,
                "partitionStatistics": [{"partitionName": " ", "statistics": []}],
            },
            {
                "code": 0,
                "partitionStatistics": [{"partitionName": "p1", "statistics": [None]}],
            },
        ]
        for field in ["name", "reserved", "modifiable", "audit"]:
            payload = copy.deepcopy(self.payload)
            del payload["partitionStatistics"][0]["statistics"][0][field]
            payloads.append(payload)
        for payload in payloads:
            with self.subTest(payload=payload), self.assertRaises(
                IllegalArgumentException
            ):
                self.decode(payload)
