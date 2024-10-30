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

from gravitino.api.types import Types, Name


class TestTypes(unittest.TestCase):

    # Test NullType singleton and methods
    def test_null_type(self):
        null_type_1 = Types.NullType.get()
        null_type_2 = Types.NullType.get()

        self.assertIs(
            null_type_1,
            null_type_2,
            "NullType should return the same singleton instance",
        )
        self.assertEqual(null_type_1.name(), Name.NULL)
        self.assertEqual(null_type_1.simpleString(), "null")

    # Test BooleanType singleton and methods
    def test_boolean_type(self):
        boolean_type_1 = Types.BooleanType.get()
        boolean_type_2 = Types.BooleanType.get()

        self.assertIs(
            boolean_type_1,
            boolean_type_2,
            "BooleanType should return the same singleton instance",
        )
        self.assertEqual(boolean_type_1.name(), Name.BOOLEAN)
        self.assertEqual(boolean_type_1.simpleString(), "boolean")

    # Test ByteType for signed and unsigned versions
    def test_byte_type(self):
        signed_byte = Types.ByteType.get()
        unsigned_byte = Types.ByteType.unsigned()

        self.assertEqual(signed_byte.name(), Name.BYTE)
        self.assertEqual(signed_byte.simpleString(), "byte")
        self.assertEqual(unsigned_byte.simpleString(), "byte unsigned")

    # Test ShortType for signed and unsigned versions
    def test_short_type(self):
        signed_short = Types.ShortType.get()
        unsigned_short = Types.ShortType.unsigned()

        self.assertEqual(signed_short.name(), Name.SHORT)
        self.assertEqual(signed_short.simpleString(), "short")
        self.assertEqual(unsigned_short.simpleString(), "short unsigned")

    # Test IntegerType for signed and unsigned versions
    def test_integer_type(self):
        signed_int = Types.IntegerType.get()
        unsigned_int = Types.IntegerType.unsigned()

        self.assertEqual(signed_int.name(), Name.INTEGER)
        self.assertEqual(signed_int.simpleString(), "integer")
        self.assertEqual(unsigned_int.simpleString(), "integer unsigned")

    # Test DecimalType and its methods
    def test_decimal_type(self):
        decimal_type_1 = Types.DecimalType.of(10, 2)
        decimal_type_2 = Types.DecimalType.of(10, 2)
        decimal_type_3 = Types.DecimalType.of(12, 3)

        self.assertEqual(decimal_type_1.name(), Name.DECIMAL)
        self.assertEqual(decimal_type_1.simpleString(), "decimal(10,2)")
        self.assertEqual(
            decimal_type_1,
            decimal_type_2,
            "Decimal types with the same precision and scale should be equal",
        )
        self.assertNotEqual(
            decimal_type_1,
            decimal_type_3,
            "Decimal types with different precision/scale should not be equal",
        )
        self.assertNotEqual(
            hash(decimal_type_1),
            hash(decimal_type_3),
            "Different decimal types should have different hash codes",
        )

    # Test DateType singleton and methods
    def test_date_type(self):
        date_type_1 = Types.DateType.get()
        date_type_2 = Types.DateType.get()

        self.assertIs(
            date_type_1,
            date_type_2,
            "DateType should return the same singleton instance",
        )
        self.assertEqual(date_type_1.name(), Name.DATE)
        self.assertEqual(date_type_1.simpleString(), "date")

    # Test TimeType singleton and methods
    def test_time_type(self):
        time_type_1 = Types.TimeType.get()
        time_type_2 = Types.TimeType.get()

        self.assertIs(
            time_type_1,
            time_type_2,
            "TimeType should return the same singleton instance",
        )
        self.assertEqual(time_type_1.name(), Name.TIME)
        self.assertEqual(time_type_1.simpleString(), "time")

    # Test StringType singleton and methods
    def test_string_type(self):
        string_type_1 = Types.StringType.get()
        string_type_2 = Types.StringType.get()

        self.assertIs(
            string_type_1,
            string_type_2,
            "StringType should return the same singleton instance",
        )
        self.assertEqual(string_type_1.name(), Name.STRING)
        self.assertEqual(string_type_1.simpleString(), "string")

    # Test UUIDType singleton and methods
    def test_uuid_type(self):
        uuid_type_1 = Types.UUIDType.get()
        uuid_type_2 = Types.UUIDType.get()

        self.assertIs(
            uuid_type_1,
            uuid_type_2,
            "UUIDType should return the same singleton instance",
        )
        self.assertEqual(uuid_type_1.name(), Name.UUID)
        self.assertEqual(uuid_type_1.simpleString(), "uuid")

    # Test FixedType creation and equality
    def test_fixed_type(self):
        fixed_type_1 = Types.FixedType.of(16)
        fixed_type_2 = Types.FixedType.of(16)
        fixed_type_3 = Types.FixedType.of(32)

        self.assertEqual(fixed_type_1.name(), Name.FIXED)
        self.assertEqual(fixed_type_1.simpleString(), "fixed(16)")
        self.assertEqual(fixed_type_1, fixed_type_2)
        self.assertNotEqual(fixed_type_1, fixed_type_3)

    # Test VarCharType creation and equality
    def test_varchar_type(self):
        varchar_type_1 = Types.VarCharType.of(255)
        varchar_type_2 = Types.VarCharType.of(255)
        varchar_type_3 = Types.VarCharType.of(512)

        self.assertEqual(varchar_type_1.name(), Name.VARCHAR)
        self.assertEqual(varchar_type_1.simpleString(), "varchar(255)")
        self.assertEqual(varchar_type_1, varchar_type_2)
        self.assertNotEqual(varchar_type_1, varchar_type_3)

    # Test allowAutoIncrement method for IntegerType and LongType
    def test_allow_auto_increment(self):
        integer_type = Types.IntegerType.get()
        long_type = Types.LongType.get()
        boolean_type = Types.BooleanType.get()

        self.assertTrue(Types.allowAutoIncrement(integer_type))
        self.assertTrue(Types.allowAutoIncrement(long_type))
        self.assertFalse(Types.allowAutoIncrement(boolean_type))
