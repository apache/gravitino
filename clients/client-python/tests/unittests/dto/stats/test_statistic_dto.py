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

from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.stats.statistic_dto import StatisticDTO


class TestStatisticDTOSerDes(unittest.TestCase):
    def test_statistic_dto_serdes_without_value(self):
        statistic_dto = StatisticDTO(
            _name="test_statistic",
            _reserved=True,
            _modifiable=False,
            _audit=AuditDTO(_creator="test_creator"),
        )
        statistic_dto.validate()
        json_str = statistic_dto.to_json()
        json_obj = json.loads(json_str)
        self.assertEqual(json_obj["name"], statistic_dto.name())
        self.assertEqual(json_obj["reserved"], statistic_dto.reserved())
        self.assertEqual(json_obj["modifiable"], statistic_dto.modifiable())
        self.assertEqual(
            json_obj["audit"]["creator"], statistic_dto.audit_info().creator()
        )
        self.assertNotIn("value", json_obj)

        deserialized_statistic_dto = StatisticDTO.from_json(json_str)
        self.assertEqual(deserialized_statistic_dto, statistic_dto)

    def test_statistic_dto_serdes_with_value(self):
        statistic_dto = StatisticDTO(
            _name="test_statistic_with_value",
            _reserved=False,
            _modifiable=True,
            _audit=AuditDTO(_creator="test_creator"),
            _value=StatisticValues.list_value(
                [StatisticValues.long_value(v) for v in range(5)]
            ),
        )
        json_str = statistic_dto.to_json()
        json_obj = json.loads(json_str)
        self.assertEqual(json_obj["name"], statistic_dto.name())
        self.assertEqual(json_obj["reserved"], statistic_dto.reserved())
        self.assertEqual(json_obj["modifiable"], statistic_dto.modifiable())
        self.assertEqual(
            json_obj["audit"]["creator"], statistic_dto.audit_info().creator()
        )
        self.assertEqual(json_obj["value"], list(range(5)))

        deserialized_statistic_dto = StatisticDTO.from_json(json_str)
        self.assertEqual(deserialized_statistic_dto, statistic_dto)
