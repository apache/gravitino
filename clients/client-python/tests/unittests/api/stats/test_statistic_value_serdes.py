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

from gravitino.api.stats.json_serdes.statistic_value_serdes import (
    StatisticValueSerdes,
)
from gravitino.api.stats.statistic_values import StatisticValues


class TestStatisticValueJsonSerdes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._rand_int = random.randint(0, 500)
        cls._rand_float = random.uniform(0, 500)
        cls._rand_str = f"str-{cls._rand_int}"

    def test_deserialize_naive_types(self):
        self.assertEqual(
            StatisticValueSerdes.deserialize(self._rand_int),
            StatisticValues.long_value(self._rand_int),
        )
        self.assertEqual(
            StatisticValueSerdes.deserialize(self._rand_float),
            StatisticValues.double_value(self._rand_float),
        )
        self.assertEqual(
            StatisticValueSerdes.deserialize(self._rand_str),
            StatisticValues.string_value(self._rand_str),
        )
        self.assertEqual(
            StatisticValueSerdes.deserialize(True), StatisticValues.boolean_value(True)
        )

    def test_deserialize_object_type(self):
        data = {
            "boolean": True,
            "long": self._rand_int,
            "double": self._rand_float,
            "string": self._rand_str,
            "list": [self._rand_int, self._rand_int + 1, self._rand_int + 2],
            "struct": {
                "nested_boolean": False,
                "nested_string": "nested",
                "nested_list": [self._rand_str, self._rand_str + "_another"],
            },
        }

        deserialized_value = StatisticValueSerdes.deserialize(data)

        expected_value = StatisticValues.object_value(
            {
                "boolean": StatisticValues.boolean_value(True),
                "long": StatisticValues.long_value(self._rand_int),
                "double": StatisticValues.double_value(self._rand_float),
                "string": StatisticValues.string_value(self._rand_str),
                "list": StatisticValues.list_value(
                    [
                        StatisticValues.long_value(self._rand_int),
                        StatisticValues.long_value(self._rand_int + 1),
                        StatisticValues.long_value(self._rand_int + 2),
                    ]
                ),
                "struct": StatisticValues.object_value(
                    {
                        "nested_boolean": StatisticValues.boolean_value(False),
                        "nested_string": StatisticValues.string_value("nested"),
                        "nested_list": StatisticValues.list_value(
                            [
                                StatisticValues.string_value(self._rand_str),
                                StatisticValues.string_value(
                                    self._rand_str + "_another"
                                ),
                            ]
                        ),
                    }
                ),
            }
        )

        self.assertEqual(deserialized_value, expected_value)

    def test_deserialize_unsupported_type(self):
        with self.assertRaises(ValueError):
            StatisticValueSerdes.deserialize(None)

    def test_serialize_naive_types(self):
        self.assertEqual(
            StatisticValueSerdes.serialize(StatisticValues.long_value(self._rand_int)),
            self._rand_int,
        )
        self.assertEqual(
            StatisticValueSerdes.serialize(
                StatisticValues.double_value(self._rand_float)
            ),
            self._rand_float,
        )
        self.assertEqual(
            StatisticValueSerdes.serialize(
                StatisticValues.string_value(self._rand_str)
            ),
            self._rand_str,
        )
        self.assertEqual(
            StatisticValueSerdes.serialize(StatisticValues.boolean_value(True)), True
        )

    def test_serialize_object_type(self):
        value = StatisticValues.object_value(
            {
                "boolean": StatisticValues.boolean_value(True),
                "long": StatisticValues.long_value(self._rand_int),
                "double": StatisticValues.double_value(self._rand_float),
                "string": StatisticValues.string_value(self._rand_str),
                "list": StatisticValues.list_value(
                    [
                        StatisticValues.long_value(self._rand_int),
                        StatisticValues.long_value(self._rand_int + 1),
                        StatisticValues.long_value(self._rand_int + 2),
                    ]
                ),
                "struct": StatisticValues.object_value(
                    {
                        "nested_boolean": StatisticValues.boolean_value(False),
                        "nested_string": StatisticValues.string_value("nested"),
                        "nested_list": StatisticValues.list_value(
                            [
                                StatisticValues.string_value(self._rand_str),
                                StatisticValues.string_value(
                                    self._rand_str + "_another"
                                ),
                            ]
                        ),
                    }
                ),
            }
        )

        serialized_data = StatisticValueSerdes.serialize(value)

        expected_data = {
            "boolean": True,
            "long": self._rand_int,
            "double": self._rand_float,
            "string": self._rand_str,
            "list": [self._rand_int, self._rand_int + 1, self._rand_int + 2],
            "struct": {
                "nested_boolean": False,
                "nested_string": "nested",
                "nested_list": [self._rand_str, self._rand_str + "_another"],
            },
        }

        self.assertIsInstance(serialized_data, dict)
        self.assertDictEqual(serialized_data, expected_data)
