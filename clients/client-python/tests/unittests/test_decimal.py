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
from decimal import Decimal as PyDecimal, InvalidOperation
from gravitino.api.decimal import Decimal


class TestDecimal(unittest.TestCase):
    def test_decimal_creation_with_precision_and_scale(self):
        dec = Decimal.of("123.45", 5, 2)
        self.assertEqual(dec.value, PyDecimal("123.45"))
        self.assertEqual(dec.precision, 5)
        self.assertEqual(dec.scale, 2)

    def test_decimal_creation_without_precision_and_scale(self):
        # With the new precision calculation, this should yield precision 6 (includes trailing zero)
        dec = Decimal.of("123.450")
        self.assertEqual(dec.value, PyDecimal("123.450"))
        self.assertEqual(dec.precision, 6)
        self.assertEqual(dec.scale, 3)

    def test_decimal_precision_limit(self):
        # Testing edge cases at precision limits
        dec = Decimal.of("1.2345678901234567890123456789012345678", 38, 0)
        self.assertEqual(dec.precision, 38)
        self.assertEqual(dec.scale, 0)
        with self.assertRaises(ValueError):
            Decimal.of("1.23456789012345678901234567890123456789", 39, 0)

    def test_decimal_scale_limit(self):
        # Testing maximum scale allowed by precision
        dec = Decimal.of("0.12345678901234567890123456", 38, 28)
        self.assertEqual(dec.precision, 38)
        self.assertEqual(dec.scale, 28)

    def test_rounding_behavior(self):
        dec = Decimal.of("123.4567", 6, 2)
        self.assertEqual(dec.value, PyDecimal("123.46"))  # rounded to scale 2

    def test_invalid_precision_scale_combination(self):
        with self.assertRaises(ValueError):
            Decimal.of("12.345", 2, 3)  # Scale exceeds precision

    def test_equality_and_hashing(self):
        dec1 = Decimal.of("123.45", 5, 2)
        dec2 = Decimal.of("123.45", 5, 2)
        dec3 = Decimal.of("123.450", 6, 3)
        self.assertEqual(dec1, dec2)
        self.assertNotEqual(dec1, dec3)
        self.assertEqual(hash(dec1), hash(dec2))

    def test_string_representation(self):
        dec = Decimal.of("123.45", 5, 2)
        self.assertEqual(str(dec), "123.45")

    def test_invalid_value_raises_error(self):
        with self.assertRaises(InvalidOperation):
            Decimal.of("invalid")

    def test_large_and_small_values(self):
        dec_small = Decimal.of("0.00000001", 10, 8)
        dec_large = Decimal.of("12345678901234567890", 20, 0)
        self.assertEqual(dec_small.value, PyDecimal("0.00000001"))
        self.assertEqual(dec_large.value, PyDecimal("12345678901234567890"))

    def test_precision_scale_edge_cases(self):
        dec = Decimal.of("1.1", 2, 1)  # Adjusted to include trailing zero significance
        self.assertEqual(dec.precision, 2)
        self.assertEqual(dec.scale, 1)
        with self.assertRaises(ValueError):
            Decimal.of("1.1", 0, 1)  # Invalid precision lower bound

    def test_decimal_with_set_scale(self):
        # Test if value is correctly set to required scale with rounding
        dec = Decimal.of("123.4", 5, 3)
        self.assertEqual(dec.value, PyDecimal("123.400"))
