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
from unittest.mock import MagicMock

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import FunctionExpression
from gravitino.api.rel.expressions.named_reference import NamedReference
from gravitino.api.rel.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.rel.expressions.sorts.sort_direction import SortDirection
from gravitino.api.rel.expressions.sorts.sort_orders import SortImpl, SortOrders


class TestSortOrder(unittest.TestCase):
    def test_sort_direction_from_string(self):
        self.assertEqual(SortDirection.from_string("asc"), SortDirection.ASCENDING)
        self.assertEqual(SortDirection.from_string("desc"), SortDirection.DESCENDING)
        with self.assertRaises(ValueError):
            SortDirection.from_string("invalid")

    def test_null_ordering(self):
        self.assertEqual(str(NullOrdering.NULLS_FIRST), "nulls_first")
        self.assertEqual(str(NullOrdering.NULLS_LAST), "nulls_last")

    def test_sort_impl_initialization(self):
        mock_expression = MagicMock(spec=Expression)
        sort_impl = SortImpl(
            expression=mock_expression,
            direction=SortDirection.ASCENDING,
            null_ordering=NullOrdering.NULLS_FIRST,
        )
        self.assertEqual(sort_impl.expression(), mock_expression)
        self.assertEqual(sort_impl.direction(), SortDirection.ASCENDING)
        self.assertEqual(sort_impl.null_ordering(), NullOrdering.NULLS_FIRST)

    def test_sort_impl_equality(self):
        mock_expression1 = MagicMock(spec=Expression)
        mock_expression2 = MagicMock(spec=Expression)

        sort_impl1 = SortImpl(
            expression=mock_expression1,
            direction=SortDirection.ASCENDING,
            null_ordering=NullOrdering.NULLS_FIRST,
        )
        sort_impl2 = SortImpl(
            expression=mock_expression1,
            direction=SortDirection.ASCENDING,
            null_ordering=NullOrdering.NULLS_FIRST,
        )
        sort_impl3 = SortImpl(
            expression=mock_expression2,
            direction=SortDirection.ASCENDING,
            null_ordering=NullOrdering.NULLS_FIRST,
        )

        self.assertEqual(sort_impl1, sort_impl2)
        self.assertNotEqual(sort_impl1, sort_impl3)

    def test_sort_orders(self):
        mock_expression = MagicMock(spec=Expression)
        ascending_order = SortOrders.ascending(mock_expression)
        self.assertEqual(ascending_order.direction(), SortDirection.ASCENDING)
        self.assertEqual(ascending_order.null_ordering(), NullOrdering.NULLS_FIRST)

        descending_order = SortOrders.descending(mock_expression)
        self.assertEqual(descending_order.direction(), SortDirection.DESCENDING)
        self.assertEqual(descending_order.null_ordering(), NullOrdering.NULLS_LAST)

    def test_sort_impl_string_representation(self):
        mock_expression = MagicMock(spec=Expression)
        sort_impl = SortImpl(
            expression=mock_expression,
            direction=SortDirection.ASCENDING,
            null_ordering=NullOrdering.NULLS_FIRST,
        )
        expected_str = (
            f"SortImpl(expression={mock_expression}, "
            f"direction=asc, null_ordering=nulls_first)"
        )
        self.assertEqual(str(sort_impl), expected_str)

    def test_sort_order(self):
        field_reference = NamedReference.field(["field1"])
        sort_order = SortOrders.of(
            field_reference, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST
        )

        self.assertEqual(NullOrdering.NULLS_FIRST, sort_order.null_ordering())
        self.assertEqual(SortDirection.ASCENDING, sort_order.direction())
        self.assertIsInstance(sort_order.expression(), NamedReference)
        self.assertEqual(["field1"], sort_order.expression().field_name())

        date = FunctionExpression.of("date", NamedReference.field(["b"]))
        sort_order = SortOrders.of(
            date, SortDirection.DESCENDING, NullOrdering.NULLS_LAST
        )
        self.assertEqual(NullOrdering.NULLS_LAST, sort_order.null_ordering())
        self.assertEqual(SortDirection.DESCENDING, sort_order.direction())

        self.assertIsInstance(sort_order.expression(), FunctionExpression)
        self.assertEqual("date", sort_order.expression().function_name())
        self.assertEqual(
            ["b"], sort_order.expression().arguments()[0].references()[0].field_name()
        )
