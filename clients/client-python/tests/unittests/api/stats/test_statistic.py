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

from gravitino.api.audit import Audit
from gravitino.api.rel.types.types import Types
from gravitino.api.stats.statistic import Statistic
from gravitino.api.stats.statistic_value import StatisticValue


class TestStatistic(unittest.TestCase):
    def test_importability(self):
        """Test that Statistic can be imported."""
        self.assertIsNotNone(Statistic)

    def test_custom_prefix_constant(self):
        """Test that CUSTOM_PREFIX constant is defined correctly."""
        self.assertEqual("custom-", Statistic.CUSTOM_PREFIX)

    def test_is_abstract(self):
        """Test that Statistic is an abstract base class."""
        with self.assertRaises(TypeError):
            Statistic()  # pylint: disable=abstract-class-instantiated

    def test_concrete_implementation(self):
        """Test that a concrete implementation can be created."""

        class ConcreteStatisticValue(StatisticValue[int]):
            def value(self) -> int:
                return 100

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
            def name(self) -> str:
                return "row_count"

            def value(self) -> StatisticValue[int] | None:
                return ConcreteStatisticValue()

            def reserved(self) -> bool:
                return True

            def modifiable(self) -> bool:
                return False

            def audit_info(self) -> Audit:
                return ConcreteAudit()

        instance = ConcreteStatistic()
        self.assertEqual("row_count", instance.name())
        self.assertIsNotNone(instance.value())
        self.assertEqual(100, instance.value().value())
        self.assertTrue(instance.reserved())
        self.assertFalse(instance.modifiable())
        self.assertIsNotNone(instance.audit_info())
