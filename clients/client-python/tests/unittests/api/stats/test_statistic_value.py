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

from gravitino.api.rel.types.types import Types
from gravitino.api.stats.statistic_value import StatisticValue


class TestStatisticValue(unittest.TestCase):
    def test_importability(self):
        """Test that StatisticValue can be imported."""
        self.assertIsNotNone(StatisticValue)

    def test_is_abstract(self):
        """Test that StatisticValue is an abstract base class."""
        with self.assertRaises(TypeError):
            StatisticValue()  # pylint: disable=abstract-class-instantiated

    def test_concrete_implementation(self):
        """Test that a concrete implementation can be created."""

        class ConcreteStatisticValue(StatisticValue[int]):
            def value(self) -> int:
                return 42

            def data_type(self) -> Types.IntegerType:
                return Types.IntegerType.get()

        instance = ConcreteStatisticValue()
        self.assertEqual(42, instance.value())
        self.assertEqual(Types.IntegerType.get(), instance.data_type())
