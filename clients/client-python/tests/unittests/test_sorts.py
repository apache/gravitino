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
from gravitino.api.expressions.sorts import (
    SortDirection,
    NullOrdering,
    SortImpl,
    SortOrders,
)


class TestSortImpl(unittest.TestCase):

    def test_sortimpl_initialization(self):
        # Testing initialization of SortImpl
        expr = "column_name"
        direction = SortDirection.ASCENDING
        null_ordering = NullOrdering.NULLS_FIRST
        sort_order = SortImpl(expr, direction, null_ordering)

        self.assertEqual(sort_order.expression(), expr)
        self.assertEqual(sort_order.direction(), direction)
        self.assertEqual(sort_order.null_ordering(), null_ordering)

    def test_sortimpl_equality(self):
        # Testing equality of two SortImpl instances
        expr = "column_name"
        direction = SortDirection.ASCENDING
        null_ordering = NullOrdering.NULLS_FIRST
        sort_order1 = SortImpl(expr, direction, null_ordering)
        sort_order2 = SortImpl(expr, direction, null_ordering)

        self.assertEqual(sort_order1, sort_order2)

    def test_sortimpl_inequality(self):
        # Testing inequality of two SortImpl instances
        expr = "column_name"
        direction = SortDirection.ASCENDING
        null_ordering = NullOrdering.NULLS_FIRST
        sort_order1 = SortImpl(expr, direction, null_ordering)
        sort_order2 = SortImpl("another_column", direction, null_ordering)

        self.assertNotEqual(sort_order1, sort_order2)

    def test_sortimpl_hash(self):
        # Testing the hash method of SortImpl
        expr = "column_name"
        direction = SortDirection.ASCENDING
        null_ordering = NullOrdering.NULLS_FIRST
        sort_order1 = SortImpl(expr, direction, null_ordering)
        sort_order2 = SortImpl(expr, direction, null_ordering)

        self.assertEqual(hash(sort_order1), hash(sort_order2))


class TestSortDirection(unittest.TestCase):

    def test_from_string(self):
        # Test from_string method for SortDirection
        self.assertEqual(SortDirection.from_string("asc"), SortDirection.ASCENDING)
        self.assertEqual(SortDirection.from_string("desc"), SortDirection.DESCENDING)

        with self.assertRaises(ValueError):
            SortDirection.from_string("invalid")

    def test_default_null_ordering(self):
        # Test default_null_ordering method for SortDirection
        self.assertEqual(
            SortDirection.ASCENDING.default_null_ordering(), NullOrdering.NULLS_FIRST
        )
        self.assertEqual(
            SortDirection.DESCENDING.default_null_ordering(), NullOrdering.NULLS_LAST
        )


class TestNullOrdering(unittest.TestCase):

    def test_from_string(self):
        # Test from_string method for NullOrdering
        self.assertEqual(
            NullOrdering.from_string("nulls_first"), NullOrdering.NULLS_FIRST
        )
        self.assertEqual(
            NullOrdering.from_string("nulls_last"), NullOrdering.NULLS_LAST
        )

        with self.assertRaises(ValueError):
            NullOrdering.from_string("invalid")


class TestSortOrders(unittest.TestCase):

    def test_ascending(self):
        # Test the ascending method of SortOrders
        expr = "column_name"
        sort_order = SortOrders.ascending(expr)
        self.assertEqual(sort_order.expression(), expr)
        self.assertEqual(sort_order.direction(), SortDirection.ASCENDING)
        self.assertEqual(sort_order.null_ordering(), NullOrdering.NULLS_FIRST)

    def test_descending(self):
        # Test the descending method of SortOrders
        expr = "column_name"
        sort_order = SortOrders.descending(expr)
        self.assertEqual(sort_order.expression(), expr)
        self.assertEqual(sort_order.direction(), SortDirection.DESCENDING)
        self.assertEqual(sort_order.null_ordering(), NullOrdering.NULLS_LAST)

    def test_of(self):
        # Test the of method of SortOrders
        expr = "column_name"
        sort_order = SortOrders.of(
            expr, SortDirection.DESCENDING, NullOrdering.NULLS_FIRST
        )
        self.assertEqual(sort_order.expression(), expr)
        self.assertEqual(sort_order.direction(), SortDirection.DESCENDING)
        self.assertEqual(sort_order.null_ordering(), NullOrdering.NULLS_FIRST)

    def test_from_string(self):
        # Test the from_string method of SortOrders
        expr = "column_name"
        sort_order = SortOrders.from_string(expr, "asc", "nulls_last")
        self.assertEqual(sort_order.expression(), expr)
        self.assertEqual(sort_order.direction(), SortDirection.ASCENDING)
        self.assertEqual(sort_order.null_ordering(), NullOrdering.NULLS_LAST)
