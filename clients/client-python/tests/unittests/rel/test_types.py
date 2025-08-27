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

from gravitino.api.types.types import Types, Name


class TestTypes(unittest.TestCase):
    def test_null_type(self):
        instance: Types.NullType = Types.NullType.get()
        self.assertIsInstance(instance, Types.NullType)
        self.assertEqual(instance.name(), Name.NULL)
        self.assertEqual(instance.simple_string(), "null")
        self.assertIs(instance, Types.NullType.get())  # Singleton check

    def test_boolean_type(self):
        instance: Types.BooleanType = Types.BooleanType.get()
        self.assertIsInstance(instance, Types.BooleanType)
        self.assertEqual(instance.name(), Name.BOOLEAN)
        self.assertEqual(instance.simple_string(), "boolean")
        self.assertIs(instance, Types.BooleanType.get())  # Singleton check

    def test_byte_type(self):
        signed_instance: Types.ByteType = Types.ByteType.get()
        unsigned_instance = Types.ByteType.unsigned()
        self.assertIsInstance(signed_instance, Types.ByteType)
        self.assertEqual(signed_instance.name(), Name.BYTE)
        self.assertEqual(signed_instance.simple_string(), "byte")
        self.assertEqual(unsigned_instance.simple_string(), "byte unsigned")

    def test_short_type(self):
        signed_instance: Types.ShortType = Types.ShortType.get()
        unsigned_instance = Types.ShortType.unsigned()
        self.assertIsInstance(signed_instance, Types.ShortType)
        self.assertEqual(signed_instance.simple_string(), "short")
        self.assertEqual(unsigned_instance.simple_string(), "short unsigned")

    def test_integer_type(self):
        signed_instance: Types.IntegerType = Types.IntegerType.get()
        unsigned_instance = Types.IntegerType.unsigned()
        self.assertIsInstance(signed_instance, Types.IntegerType)
        self.assertEqual(signed_instance.simple_string(), "integer")
        self.assertEqual(unsigned_instance.simple_string(), "integer unsigned")

    def test_long_type(self):
        signed_instance: Types.LongType = Types.LongType.get()
        unsigned_instance = Types.LongType.unsigned()
        self.assertIsInstance(signed_instance, Types.LongType)
        self.assertEqual(signed_instance.simple_string(), "long")
        self.assertEqual(unsigned_instance.simple_string(), "long unsigned")

    def test_float_type(self):
        instance: Types.FloatType = Types.FloatType.get()
        self.assertEqual(instance.name(), Name.FLOAT)
        self.assertEqual(instance.simple_string(), "float")

    def test_double_type(self):
        instance: Types.DoubleType = Types.DoubleType.get()
        self.assertEqual(instance.name(), Name.DOUBLE)
        self.assertEqual(instance.simple_string(), "double")

    def test_decimal_type(self):
        instance: Types.DecimalType = Types.DecimalType.of(10, 2)
        self.assertEqual(instance.name(), Name.DECIMAL)
        self.assertEqual(instance.precision(), 10)
        self.assertEqual(instance.scale(), 2)
        self.assertEqual(instance.simple_string(), "decimal(10,2)")
        with self.assertRaises(ValueError):
            Types.DecimalType.of(39, 2)  # Precision out of range
        with self.assertRaises(ValueError):
            Types.DecimalType.of(10, 11)  # Scale out of range

    def test_date_type(self):
        instance: Types.DateType = Types.DateType.get()
        self.assertEqual(instance.name(), Name.DATE)
        self.assertEqual(instance.simple_string(), "date")

    def test_time_type(self):
        # Test TimeType without precision
        instance: Types.TimeType = Types.TimeType.get()
        self.assertEqual(instance.name(), Name.TIME)
        self.assertEqual(instance.simple_string(), "time")
        self.assertFalse(instance.has_precision_set())
        self.assertEqual(
            instance.precision(), Types.TimeType.DATE_TIME_PRECISION_NOT_SET
        )

        # Test TimeType with precision
        time_with_precision = Types.TimeType.of(6)
        self.assertEqual(time_with_precision.name(), Name.TIME)
        self.assertEqual(time_with_precision.simple_string(), "time(6)")
        self.assertTrue(time_with_precision.has_precision_set())
        self.assertEqual(time_with_precision.precision(), 6)

        # Test Precision Range Verification
        with self.assertRaises(ValueError):
            Types.TimeType.of(-1)
        with self.assertRaises(ValueError):
            Types.TimeType.of(13)

        time_with_precision_2 = Types.TimeType.of(6)
        self.assertEqual(time_with_precision, time_with_precision_2)
        self.assertEqual(hash(time_with_precision), hash(time_with_precision_2))

        time_different_precision = Types.TimeType.of(3)
        self.assertNotEqual(time_with_precision, time_different_precision)

    def test_timestamp_type(self):
        # Test TimestampType without precision
        instance_with_tz = Types.TimestampType.with_time_zone()
        instance_without_tz = Types.TimestampType.without_time_zone()
        self.assertTrue(instance_with_tz.has_time_zone())
        self.assertFalse(instance_without_tz.has_time_zone())
        self.assertEqual(instance_with_tz.simple_string(), "timestamp_tz")
        self.assertEqual(instance_without_tz.simple_string(), "timestamp")
        self.assertFalse(instance_with_tz.has_precision_set())
        self.assertFalse(instance_without_tz.has_precision_set())

        # Test TimestampType with precision (with timezone)
        timestamp_tz_with_precision = Types.TimestampType.with_time_zone(6)
        self.assertTrue(timestamp_tz_with_precision.has_time_zone())
        self.assertTrue(timestamp_tz_with_precision.has_precision_set())
        self.assertEqual(timestamp_tz_with_precision.precision(), 6)
        self.assertEqual(timestamp_tz_with_precision.simple_string(), "timestamp_tz(6)")

        # Test TimestampType with precision (without timezone)
        timestamp_with_precision = Types.TimestampType.without_time_zone(3)
        self.assertFalse(timestamp_with_precision.has_time_zone())
        self.assertTrue(timestamp_with_precision.has_precision_set())
        self.assertEqual(timestamp_with_precision.precision(), 3)
        self.assertEqual(timestamp_with_precision.simple_string(), "timestamp(3)")

        # Test Precision Range Verification
        with self.assertRaises(ValueError):
            Types.TimestampType.with_time_zone(-1)
        with self.assertRaises(ValueError):
            Types.TimestampType.without_time_zone(13)

        timestamp_tz_with_precision_2 = Types.TimestampType.with_time_zone(6)
        self.assertEqual(timestamp_tz_with_precision, timestamp_tz_with_precision_2)
        self.assertEqual(
            hash(timestamp_tz_with_precision), hash(timestamp_tz_with_precision_2)
        )

        # Different precision or timezone should not be equal
        timestamp_different_precision = Types.TimestampType.with_time_zone(3)
        self.assertNotEqual(timestamp_tz_with_precision, timestamp_different_precision)

        timestamp_different_tz = Types.TimestampType.without_time_zone(6)
        self.assertNotEqual(timestamp_tz_with_precision, timestamp_different_tz)

    def test_interval_types(self):
        year_instance: Types.IntervalYearType = Types.IntervalYearType.get()
        day_instance: Types.IntervalDayType = Types.IntervalDayType.get()
        self.assertEqual(year_instance.name(), Name.INTERVAL_YEAR)
        self.assertEqual(day_instance.name(), Name.INTERVAL_DAY)
        self.assertEqual(year_instance.simple_string(), "interval_year")
        self.assertEqual(day_instance.simple_string(), "interval_day")

    def test_string_type(self):
        instance: Types.StringType = Types.StringType.get()
        self.assertEqual(instance.name(), Name.STRING)
        self.assertEqual(instance.simple_string(), "string")

    def test_uuid_type(self):
        instance: Types.UUIDType = Types.UUIDType.get()
        self.assertEqual(instance.name(), Name.UUID)
        self.assertEqual(instance.simple_string(), "uuid")

    def test_fixed_type(self):
        instance: Types.FixedType = Types.FixedType.of(5)
        self.assertEqual(instance.name(), Name.FIXED)
        self.assertEqual(instance.length(), 5)
        self.assertEqual(instance.simple_string(), "fixed(5)")

    def test_varchar_type(self):
        instance: Types.VarCharType = Types.VarCharType.of(10)
        self.assertEqual(instance.name(), Name.VARCHAR)
        self.assertEqual(instance.length(), 10)
        self.assertEqual(instance.simple_string(), "varchar(10)")

    def test_fixed_char_type(self):
        instance: Types.FixedCharType = Types.FixedCharType.of(3)
        self.assertEqual(instance.name(), Name.FIXEDCHAR)
        self.assertEqual(instance.length(), 3)
        self.assertEqual(instance.simple_string(), "char(3)")

    def test_binary_type(self):
        instance: Types.BinaryType = Types.BinaryType.get()
        self.assertEqual(instance.name(), Name.BINARY)
        self.assertEqual(instance.simple_string(), "binary")

    def test_struct_type(self):
        field1: Types.StructType.Field = Types.StructType.Field(
            "name", Types.StringType.get(), True, "User's name"
        )
        field2: Types.StructType.Field = Types.StructType.Field(
            "age", Types.IntegerType.get(), False, "User's age"
        )
        struct: Types.StructType = Types.StructType.of(field1, field2)
        self.assertEqual(
            struct.simple_string(),
            "struct<name: string NULL COMMENT 'User's name', age: integer NOT NULL COMMENT 'User's age'>",
        )

    def test_list_type(self):
        instance: Types.ListType = Types.ListType.of(Types.StringType.get(), True)
        self.assertEqual(instance.name(), Name.LIST)
        self.assertTrue(instance.element_nullable())
        self.assertEqual(instance.simple_string(), "list<string>")

    def test_map_type(self):
        instance: Types.MapType = Types.MapType.of(
            Types.StringType.get(), Types.IntegerType.get(), True
        )
        self.assertEqual(instance.name(), Name.MAP)
        self.assertTrue(instance.is_value_nullable())
        self.assertEqual(instance.simple_string(), "map<string, integer>")

    def test_union_type(self):
        instance: Types.UnionType = Types.UnionType.of(
            Types.StringType.get(), Types.IntegerType.get()
        )
        self.assertEqual(instance.name(), Name.UNION)
        self.assertEqual(instance.simple_string(), "union<string, integer>")

    def test_unparsed_type(self):
        instance: Types.UnparsedType = Types.UnparsedType.of("custom_type")
        self.assertEqual(instance.name(), Name.UNPARSED)
        self.assertEqual(instance.simple_string(), "unparsed(custom_type)")

    def test_external_type(self):
        instance: Types.ExternalType = Types.ExternalType.of("external_type")
        self.assertEqual(instance.name(), Name.EXTERNAL)
        self.assertEqual(instance.simple_string(), "external(external_type)")

    def test_auto_increment_check(self):
        self.assertTrue(Types.allow_auto_increment(Types.IntegerType.get()))
        self.assertTrue(Types.allow_auto_increment(Types.LongType.get()))
        self.assertFalse(Types.allow_auto_increment(Types.StringType.get()))
