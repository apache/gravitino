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

from gravitino.api.rel.types.types import Types
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.exceptions.base import IllegalArgumentException


class TestStatisticValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._rand_int = random.randint(0, 500)
        cls._rand_int_another = random.randint(cls._rand_int + 1, 1000)
        cls._rand_float = random.uniform(0, 500)
        cls._rand_float_another = random.uniform(cls._rand_float + 1, 1000)
        cls._rand_str = f"str-{cls._rand_int}"
        cls._rand_str_another = f"str-{cls._rand_int_another}"

    def test_long_value(self):
        value = StatisticValues.LongValue(self._rand_int)
        twin_value = StatisticValues.long_value(self._rand_int)
        another_value = StatisticValues.LongValue(self._rand_int_another)

        self.assertEqual(value.value(), self._rand_int)
        self.assertEqual(value.data_type().name(), Types.LongType.get().name())
        self.assertEqual(hash(value), hash(self._rand_int))
        self.assertEqual(value, twin_value)
        self.assertNotEqual(value, another_value)
        self.assertNotEqual(value, StatisticValues.DoubleValue(self._rand_float))

    def test_double_value(self):
        value = StatisticValues.DoubleValue(float(self._rand_float))
        twin_value = StatisticValues.double_value(float(self._rand_float))
        another_value = StatisticValues.DoubleValue(float(self._rand_float_another))

        self.assertEqual(value.value(), float(self._rand_float))
        self.assertEqual(value.data_type().name(), Types.DoubleType.get().name())
        self.assertEqual(hash(value), hash(float(self._rand_float)))
        self.assertEqual(value, twin_value)
        self.assertNotEqual(value, another_value)
        self.assertNotEqual(value, StatisticValues.LongValue(self._rand_int))

    def test_string_value(self):
        value = StatisticValues.StringValue(self._rand_str)
        twin_value = StatisticValues.string_value(self._rand_str)
        another_value = StatisticValues.StringValue(self._rand_str_another)

        self.assertEqual(value.value(), self._rand_str)
        self.assertEqual(value.data_type().name(), Types.StringType.get().name())
        self.assertEqual(hash(value), hash(self._rand_str))
        self.assertEqual(value, twin_value)
        self.assertNotEqual(value, another_value)
        self.assertNotEqual(value, StatisticValues.LongValue(self._rand_int))

    def test_list_value(self):
        value_list: list[StatisticValue[int]] = [
            StatisticValues.LongValue(random.randint(0, 100)) for i in range(10)
        ]
        another_value_list: list[StatisticValue[int]] = [
            StatisticValues.LongValue(random.randint(0, 100)) for i in range(10)
        ]
        value = StatisticValues.ListValue(value_list)
        twin_value: StatisticValues.ListValue[int] = StatisticValues.list_value(
            value_list
        )
        another_value = StatisticValues.ListValue(another_value_list)

        self.assertEqual(value.value(), value_list)
        self.assertEqual(
            value.data_type().name(),
            Types.ListType.nullable(Types.LongType.get()).name(),
        )
        self.assertEqual(hash(value), hash(tuple(v.value() for v in value_list)))
        self.assertEqual(value, twin_value)
        self.assertNotEqual(value, another_value)
        self.assertNotEqual(value, StatisticValues.LongValue(self._rand_int))

    def test_object_value(self):
        value_dict: dict[str, StatisticValue[int]] = {
            f"key_{i}": StatisticValues.LongValue(random.randint(0, 100))
            for i in range(10)
        }
        another_value_dict: dict[str, StatisticValue[int]] = {
            f"key_{i}": StatisticValues.LongValue(random.randint(0, 100))
            for i in range(10)
        }
        value = StatisticValues.ObjectValue(value_dict)
        twin_value: StatisticValues.ObjectValue[int] = StatisticValues.object_value(
            value_dict
        )
        another_value = StatisticValues.ObjectValue(another_value_dict)

        expected_data_type = Types.StructType.of(
            *[
                Types.StructType.Field.nullable_field(key, statistic_value.data_type())
                for key, statistic_value in value_dict.items()
            ]
        )
        self.assertEqual(value.value(), value_dict)
        self.assertEqual(
            value.data_type().simple_string(), expected_data_type.simple_string()
        )
        self.assertEqual(
            hash(value), hash(tuple(v.value() for v in value_dict.values()))
        )
        self.assertEqual(value, twin_value)
        self.assertNotEqual(value, another_value)
        self.assertNotEqual(value, StatisticValues.LongValue(self._rand_int))

    def test_boolean_value(self):
        value = StatisticValues.BooleanValue(True)
        twin_value = StatisticValues.boolean_value(True)
        another_value = StatisticValues.BooleanValue(False)

        self.assertEqual(value.value(), True)
        self.assertEqual(value.data_type().name(), Types.BooleanType.get().name())
        self.assertEqual(hash(value), hash(True))
        self.assertEqual(value, twin_value)
        self.assertNotEqual(value, another_value)
        self.assertNotEqual(value, StatisticValues.LongValue(False))

    def test_object_value_empty_dict(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Values cannot be null or empty"
        ):
            StatisticValues.ObjectValue({})

    def test_list_value_empty_list(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Values cannot be null or empty"
        ):
            StatisticValues.ListValue([])

    def test_list_value_mismatched_types(self):
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "All values in the list must have the same data type",
        ):
            StatisticValues.ListValue(
                [
                    StatisticValues.LongValue(self._rand_int),
                    StatisticValues.DoubleValue(self._rand_float),
                ]
            )

    def test_list_value_nested_hash(self):
        inner_list1 = StatisticValues.list_value(
            [StatisticValues.long_value(1), StatisticValues.long_value(2)]
        )
        inner_list2 = StatisticValues.list_value(
            [StatisticValues.long_value(3), StatisticValues.long_value(4)]
        )
        nested_list = StatisticValues.list_value([inner_list1, inner_list2])
        twin_nested_list = StatisticValues.list_value([inner_list1, inner_list2])

        self.assertEqual(len(nested_list.value()), 2)
        self.assertEqual(nested_list, twin_nested_list)
        self.assertEqual(hash(nested_list), hash(twin_nested_list))

    def test_object_value_nested_hash(self):
        inner_list = StatisticValues.list_value(
            [StatisticValues.long_value(10), StatisticValues.long_value(20)]
        )
        inner_obj = StatisticValues.object_value(
            {"x": StatisticValues.long_value(100), "y": StatisticValues.long_value(200)}
        )
        nested_obj = StatisticValues.object_value(
            {
                "simple": StatisticValues.long_value(42),
                "list": inner_list,
                "object": inner_obj,
            }
        )
        twin_nested_obj = StatisticValues.object_value(
            {
                "simple": StatisticValues.long_value(42),
                "list": inner_list,
                "object": inner_obj,
            }
        )

        self.assertEqual(len(nested_obj.value()), 3)
        self.assertEqual(nested_obj, twin_nested_obj)
        self.assertEqual(hash(nested_obj), hash(twin_nested_obj))
