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
from datetime import datetime
from typing import Any

from gravitino.api.audit import Audit
from gravitino.api.rel.types.types import Types
from gravitino.api.stats.statistic import Statistic
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.api.stats.supports_statistics import SupportsStatistics


class TestSupportsStatistics(unittest.TestCase):
    def test_importability(self):
        """Test that SupportsStatistics can be imported."""
        self.assertIsNotNone(SupportsStatistics)

    def test_is_abstract(self):
        """Test that SupportsStatistics is an abstract base class."""
        with self.assertRaises(TypeError):
            SupportsStatistics()  # pylint: disable=abstract-class-instantiated

    def test_concrete_implementation(self):
        """Test that a concrete implementation can be created."""

        class ConcreteStatisticValue(StatisticValue[int]):
            def value(self) -> int:
                return 50

            def data_type(self) -> Types.IntegerType:
                return Types.IntegerType.get()

        class ConcreteAudit(Audit):
            def creator(self) -> str:
                return "test"

            def create_time(self) -> datetime:
                return datetime(2024, 1, 1)

            def last_modifier(self) -> str:
                return "test"

            def last_modified_time(self) -> datetime:
                return datetime(2024, 1, 1)

        class ConcreteStatistic(Statistic):
            def __init__(self, stat_name: str, stat_value: int):
                self._name = stat_name
                self._value = stat_value

            def name(self) -> str:
                return self._name

            def value(self) -> StatisticValue[int] | None:
                val = ConcreteStatisticValue()
                val._val = self._value  # pylint: disable=protected-access
                return val

            def reserved(self) -> bool:
                return False

            def modifiable(self) -> bool:
                return True

            def audit_info(self) -> Audit:
                return ConcreteAudit()

        class ConcreteSupportsStatistics(SupportsStatistics):
            def __init__(self):
                self._stats = {}

            def list_statistics(self) -> list[Statistic]:
                return [ConcreteStatistic(k, v.value()) for k, v in self._stats.items()]

            def update_statistics(
                self, statistics: dict[str, StatisticValue[Any]]
            ) -> None:
                self._stats.update(statistics)

            def drop_statistics(self, statistics: list[str]) -> bool:
                dropped = False
                for stat in statistics:
                    if stat in self._stats:
                        del self._stats[stat]
                        dropped = True
                return dropped

        instance = ConcreteSupportsStatistics()
        self.assertEqual([], instance.list_statistics())

        stat_val = ConcreteStatisticValue()
        instance.update_statistics({"test_stat": stat_val})
        self.assertEqual(1, len(instance.list_statistics()))

        result = instance.drop_statistics(["test_stat"])
        self.assertTrue(result)
        self.assertEqual([], instance.list_statistics())
