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
from datetime import date, time, datetime

from gravitino.api.expressions.literals.literals import Literals


class TestLiterals(unittest.TestCase):
    def test_null_literal(self):
        null_val = Literals.NULL
        self.assertEqual(null_val.value(), None)
        self.assertEqual(null_val.data_type(), "NullType")
        self.assertEqual(str(null_val), "LiteralImpl(value=None, data_type=NullType)")

    def test_boolean_literal(self):
        bool_val = Literals.boolean_literal(True)
        self.assertEqual(bool_val.value(), True)
        self.assertEqual(bool_val.data_type(), "Boolean")
        self.assertEqual(str(bool_val), "LiteralImpl(value=True, data_type=Boolean)")

    def test_integer_literal(self):
        int_val = Literals.integer_literal(42)
        self.assertEqual(int_val.value(), 42)
        self.assertEqual(int_val.data_type(), "Integer")
        self.assertEqual(str(int_val), "LiteralImpl(value=42, data_type=Integer)")

    def test_string_literal(self):
        str_val = Literals.string_literal("Hello World")
        self.assertEqual(str_val.value(), "Hello World")
        self.assertEqual(str_val.data_type(), "String")
        self.assertEqual(
            str(str_val), "LiteralImpl(value=Hello World, data_type=String)"
        )

    def test_date_literal(self):
        date_val = Literals.date_literal(date(2023, 1, 1))
        self.assertEqual(date_val.value(), date(2023, 1, 1))
        self.assertEqual(date_val.data_type(), "Date")
        self.assertEqual(str(date_val), "LiteralImpl(value=2023-01-01, data_type=Date)")

    def test_time_literal(self):
        time_val = Literals.time_literal(time(12, 30, 45))
        self.assertEqual(time_val.value(), time(12, 30, 45))
        self.assertEqual(time_val.data_type(), "Time")
        self.assertEqual(str(time_val), "LiteralImpl(value=12:30:45, data_type=Time)")

    def test_timestamp_literal(self):
        timestamp_val = Literals.timestamp_literal(datetime(2023, 1, 1, 12, 30, 45))
        self.assertEqual(timestamp_val.value(), datetime(2023, 1, 1, 12, 30, 45))
        self.assertEqual(timestamp_val.data_type(), "Timestamp")
        self.assertEqual(
            str(timestamp_val),
            "LiteralImpl(value=2023-01-01 12:30:45, data_type=Timestamp)",
        )

    def test_timestamp_literal_from_string(self):
        timestamp_val = Literals.timestamp_literal_from_string("2023-01-01T12:30:45")
        self.assertEqual(timestamp_val.value(), datetime(2023, 1, 1, 12, 30, 45))
        self.assertEqual(timestamp_val.data_type(), "Timestamp")
        self.assertEqual(
            str(timestamp_val),
            "LiteralImpl(value=2023-01-01 12:30:45, data_type=Timestamp)",
        )

    def test_varchar_literal(self):
        varchar_val = Literals.varchar_literal(10, "Test String")
        self.assertEqual(varchar_val.value(), "Test String")
        self.assertEqual(varchar_val.data_type(), "Varchar(10)")
        self.assertEqual(
            str(varchar_val), "LiteralImpl(value=Test String, data_type=Varchar(10))"
        )

    def test_equality(self):
        int_val1 = Literals.integer_literal(42)
        int_val2 = Literals.integer_literal(42)
        int_val3 = Literals.integer_literal(10)
        self.assertTrue(int_val1 == int_val2)
        self.assertFalse(int_val1 == int_val3)

    def test_hash(self):
        int_val1 = Literals.integer_literal(42)
        int_val2 = Literals.integer_literal(42)
        self.assertEqual(hash(int_val1), hash(int_val2))

    def test_unequal_literals(self):
        int_val = Literals.integer_literal(42)
        str_val = Literals.string_literal("Hello")
        self.assertFalse(int_val == str_val)
