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


import random
import unittest

from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.responses.statistic_list_response import StatisticListResponse
from gravitino.dto.stats.statistic_dto import StatisticDTO


class TestStatisticListResponse(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.str_statistic_dto = StatisticDTO(
            _name="custom-str-statistic",
            _reserved=True,
            _modifiable=False,
            _audit=AuditDTO(_creator="test_creator"),
            _value=StatisticValues.string_value("test_value"),
        )
        cls.list_statistic_dto = StatisticDTO(
            _name="custom-list-statistic",
            _reserved=True,
            _modifiable=False,
            _audit=AuditDTO(_creator="test_creator"),
            _value=StatisticValues.list_value(
                [StatisticValues.long_value(random.randint(1, 100)) for _ in range(3)]
            ),
        )
        cls.object_statistic_dto = StatisticDTO(
            _name="custom-object-statistic",
            _reserved=True,
            _modifiable=False,
            _audit=AuditDTO(_creator="test_creator"),
            _value=StatisticValues.object_value(
                {
                    "custom-double-statistic": StatisticValues.double_value(
                        random.uniform(1.0, 100.0)
                    ),
                    "custom-boolean-statistic": StatisticValues.boolean_value(
                        random.choice([True, False])
                    ),
                }
            ),
        )
        cls.statistics = [
            cls.str_statistic_dto,
            cls.list_statistic_dto,
            cls.object_statistic_dto,
        ]

    def test_statistic_list_response(self):
        response = StatisticListResponse(_code=0, _statistics=self.statistics)
        response.validate()
        statistics = response.statistics
        self.assertEqual(len(response.statistics), 3)
        self.assertIn(self.str_statistic_dto, statistics)
        self.assertIn(self.list_statistic_dto, statistics)
        self.assertIn(self.object_statistic_dto, statistics)
