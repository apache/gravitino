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
from gravitino.api.expressions.distributions.distributions import (
    Distributions,
    Strategy,
)


class TestDistribution(unittest.TestCase):
    def test_distribution_creation(self):
        # Test creating a distribution with EVEN strategy
        dist = Distributions.even(10, "a", "b")
        self.assertEqual(dist.strategy(), Strategy.EVEN)
        self.assertEqual(dist.number(), 10)
        self.assertEqual(dist.expressions(), ["a", "b"])

        # Test creating a distribution with HASH strategy
        dist_hash = Distributions.hash(5, "c", "d")
        self.assertEqual(dist_hash.strategy(), Strategy.HASH)
        self.assertEqual(dist_hash.number(), 5)
        self.assertEqual(dist_hash.expressions(), ["c", "d"])

    def test_distribution_equals(self):
        dist1 = Distributions.even(10, "a", "b")
        dist2 = Distributions.even(10, "a", "b")
        dist3 = Distributions.hash(10, "a", "b")
        self.assertTrue(dist1.equals(dist2))
        self.assertFalse(dist1.equals(dist3))

    def test_strategy_get_by_name(self):
        self.assertEqual(Strategy.get_by_name("hash"), Strategy.HASH)
        self.assertEqual(Strategy.get_by_name("RANGE"), Strategy.RANGE)
        self.assertEqual(Strategy.get_by_name("EVEN"), Strategy.EVEN)

    def test_invalid_strategy(self):
        with self.assertRaises(ValueError):
            Strategy.get_by_name("INVALID")
