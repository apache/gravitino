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
from typing import List

from gravitino.api.rel.expressions.distributions.distributions import (
    DistributionImpl,
    Distributions,
)
from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.expression import Expression


class MockExpression(Expression):
    """Mock class to simulate an Expression"""

    def children(self) -> List[Expression]:
        return Expression.EMPTY_EXPRESSION


class TestDistributions(unittest.TestCase):
    def setUp(self):
        # Create mock expressions for testing
        self.expr1 = MockExpression()  # Use the MockExpression class
        self.expr2 = MockExpression()  # Use the MockExpression class

    def test_none_distribution(self):
        # Test the NONE distribution
        distribution = Distributions.NONE
        self.assertEqual(distribution.strategy(), Strategy.NONE)
        self.assertEqual(distribution.number(), 0)
        self.assertEqual(distribution.expressions(), Expression.EMPTY_EXPRESSION)

    def test_hash_distribution(self):
        # Test the HASH distribution
        distribution = Distributions.HASH
        self.assertEqual(distribution.strategy(), Strategy.HASH)
        self.assertEqual(distribution.number(), 0)
        self.assertEqual(distribution.expressions(), Expression.EMPTY_EXPRESSION)

    def test_range_distribution(self):
        # Test the RANGE distribution
        distribution = Distributions.RANGE
        self.assertEqual(distribution.strategy(), Strategy.RANGE)
        self.assertEqual(distribution.number(), 0)
        self.assertEqual(distribution.expressions(), Expression.EMPTY_EXPRESSION)

    def test_even_distribution(self):
        # Test the EVEN distribution with multiple expressions
        distribution = Distributions.even(5, self.expr1, self.expr2)
        self.assertEqual(distribution.strategy(), Strategy.EVEN)
        self.assertEqual(distribution.number(), 5)
        self.assertEqual(distribution.expressions(), [self.expr1, self.expr2])

    def test_hash_distribution_with_multiple_expressions(self):
        # Test HASH distribution with multiple expressions
        distribution = Distributions.hash(10, self.expr1, self.expr2)
        self.assertEqual(distribution.strategy(), Strategy.HASH)
        self.assertEqual(distribution.number(), 10)
        self.assertEqual(distribution.expressions(), [self.expr1, self.expr2])

    def test_of_distribution(self):
        # Test generic distribution creation using 'of'
        distribution = Distributions.of(Strategy.RANGE, 20, self.expr1)
        self.assertEqual(distribution.strategy(), Strategy.RANGE)
        self.assertEqual(distribution.number(), 20)
        self.assertEqual(distribution.expressions(), [self.expr1])

    def test_fields_distribution(self):
        # Test the 'fields' method with multiple field names
        distribution = Distributions.fields(Strategy.HASH, 5, ["a", "b", "c"])
        self.assertEqual(distribution.strategy(), Strategy.HASH)
        self.assertEqual(distribution.number(), 5)
        self.assertTrue(
            len(distribution.expressions()) > 0
        )  # Check that fields are converted to expressions

    def test_distribution_equals(self):
        # Test the equality of two DistributionImpl instances
        distribution1 = DistributionImpl(Strategy.EVEN, 5, [self.expr1])
        distribution2 = DistributionImpl(Strategy.EVEN, 5, [self.expr1])
        distribution3 = DistributionImpl(Strategy.HASH, 10, [self.expr2])

        self.assertTrue(distribution1 == distribution2)
        self.assertFalse(distribution1 == distribution3)

    def test_distribution_hash(self):
        # Test the hash method of DistributionImpl
        distribution1 = DistributionImpl(Strategy.HASH, 5, [self.expr1])
        distribution2 = DistributionImpl(Strategy.HASH, 5, [self.expr1])
        distribution3 = DistributionImpl(Strategy.RANGE, 5, [self.expr1])

        self.assertEqual(
            hash(distribution1), hash(distribution2)
        )  # Should be equal for same values
        self.assertNotEqual(
            hash(distribution1), hash(distribution3)
        )  # Should be different for different strategy
