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
from datetime import date

from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.rel.partitions.partitions import Partitions


class TestPartitions(unittest.TestCase):
    def test_partitions(self):
        # Test RangePartition
        partition = Partitions.range(
            "p0", Literals.NULL, Literals.integer_literal(6), {}
        )
        self.assertEqual("p0", partition.name())
        self.assertEqual({}, partition.properties())
        self.assertEqual(Literals.NULL, partition.upper())
        self.assertEqual(Literals.integer_literal(6), partition.lower())

        # Test ListPartition
        partition = Partitions.list(
            "p202204_California",
            [
                [
                    Literals.date_literal(date(2022, 4, 1)),
                    Literals.string_literal("Los Angeles"),
                ],
                [
                    Literals.date_literal(date(2022, 4, 1)),
                    Literals.string_literal("San Francisco"),
                ],
            ],
            {},
        )
        self.assertEqual("p202204_California", partition.name())
        self.assertEqual({}, partition.properties())
        self.assertEqual(
            Literals.date_literal(date(2022, 4, 1)), partition.lists()[0][0]
        )
        self.assertEqual(
            Literals.string_literal("Los Angeles"), partition.lists()[0][1]
        )
        self.assertEqual(
            Literals.date_literal(date(2022, 4, 1)), partition.lists()[1][0]
        )
        self.assertEqual(
            Literals.string_literal("San Francisco"), partition.lists()[1][1]
        )

        # Test IdentityPartition
        partition = Partitions.identity(
            "dt=2008-08-08/country=us",
            [["dt"], ["country"]],
            [Literals.date_literal(date(2008, 8, 8)), Literals.string_literal("us")],
            {"location": "/user/hive/warehouse/tpch_flat_orc_2.db/orders"},
        )
        self.assertEqual("dt=2008-08-08/country=us", partition.name())
        self.assertEqual(
            {"location": "/user/hive/warehouse/tpch_flat_orc_2.db/orders"},
            partition.properties(),
        )
        self.assertEqual(["dt"], partition.field_names()[0])
        self.assertEqual(["country"], partition.field_names()[1])
        self.assertEqual(Literals.date_literal(date(2008, 8, 8)), partition.values()[0])
        self.assertEqual(Literals.string_literal("us"), partition.values()[1])

    def test_eq(self):
        """
        Test the correctness of the __eq__ method.
        """
        partition1 = Partitions.range(
            "p1", Literals.NULL, Literals.integer_literal(6), {}
        )
        partition2 = Partitions.range(
            "p1", Literals.NULL, Literals.integer_literal(6), {}
        )
        partition3 = Partitions.range(
            "p2", Literals.NULL, Literals.integer_literal(10), {}
        )

        # Test same objects are equal
        self.assertEqual(partition1, partition2)  # Should be equal
        self.assertNotEqual(partition1, partition3)  # Should not be equal

        # Test different objects are not equal
        partition4 = Partitions.range(
            "p1", Literals.NULL, Literals.integer_literal(10), {}
        )
        self.assertNotEqual(partition1, partition4)

        # Test comparison with different types
        self.assertNotEqual(partition1, "not_a_partition")  # Different type
        self.assertNotEqual(partition1, None)  # NoneType
