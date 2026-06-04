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

from gravitino.api.stats.partition_range import ALL_PARTITIONS, PartitionRange
from gravitino.exceptions.base import IllegalArgumentException


class TestPartitionRange(unittest.TestCase):
    def test_partition_range(self) -> None:
        range1: PartitionRange = PartitionRange.down_to(
            "partition1", PartitionRange.BoundType.OPEN
        )
        self.assertIsNotNone(range1.lower_partition_name())
        self.assertEqual("partition1", range1.lower_partition_name())
        self.assertEqual(PartitionRange.BoundType.OPEN, range1.lower_bound_type())
        self.assertIsNotNone(range1.lower_bound_type())
        self.assertIsNone(range1.upper_partition_name())
        self.assertIsNone(range1.upper_bound_type())
        self.assertIsNotNone(range1.comparator())

        range2: PartitionRange = PartitionRange.up_to(
            "partition3", PartitionRange.BoundType.CLOSED
        )
        self.assertIsNotNone(range2.upper_partition_name())
        self.assertEqual("partition3", range2.upper_partition_name())
        self.assertEqual(PartitionRange.BoundType.CLOSED, range2.upper_bound_type())
        self.assertIsNotNone(range2.upper_bound_type())
        self.assertIsNone(range2.lower_partition_name())
        self.assertIsNone(range2.lower_bound_type())
        self.assertIsNotNone(range2.comparator())

        range3: PartitionRange = PartitionRange.between(
            "partition1",
            PartitionRange.BoundType.OPEN,
            "partition3",
            PartitionRange.BoundType.CLOSED,
        )
        self.assertIsNotNone(range3.lower_partition_name())
        self.assertEqual("partition1", range3.lower_partition_name())
        self.assertIsNotNone(range3.lower_bound_type())
        self.assertEqual(PartitionRange.BoundType.OPEN, range3.lower_bound_type())
        self.assertIsNotNone(range3.upper_partition_name())
        self.assertEqual("partition3", range3.upper_partition_name())
        self.assertIsNotNone(range3.upper_bound_type())
        self.assertEqual(PartitionRange.BoundType.CLOSED, range3.upper_bound_type())
        self.assertIsNotNone(range3.comparator())

        range4: PartitionRange = PartitionRange.between(
            "partition1",
            PartitionRange.BoundType.CLOSED,
            "partition3",
            PartitionRange.BoundType.OPEN,
        )
        self.assertIsNotNone(range4.lower_partition_name())
        self.assertEqual("partition1", range4.lower_partition_name())
        self.assertIsNotNone(range4.lower_bound_type())
        self.assertEqual(PartitionRange.BoundType.CLOSED, range4.lower_bound_type())
        self.assertIsNotNone(range4.upper_partition_name())
        self.assertEqual("partition3", range4.upper_partition_name())
        self.assertIsNotNone(range4.upper_bound_type())
        self.assertEqual(PartitionRange.BoundType.OPEN, range4.upper_bound_type())
        self.assertIsNotNone(range4.comparator())

    def test_create_down_range_with_null_arguments(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PartitionRange.down_to(None, PartitionRange.BoundType.CLOSED)  # type: ignore

        with self.assertRaises(IllegalArgumentException):
            PartitionRange.down_to("partition1", None)  # type: ignore

    def test_create_up_range_with_null_arguments(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PartitionRange.up_to(None, PartitionRange.BoundType.CLOSED)  # type: ignore

        with self.assertRaises(IllegalArgumentException):
            PartitionRange.up_to("partition1", None)  # type: ignore

    def test_up_to_with_null_comparator(self) -> None:
        down_closed_range = PartitionRange.down_to(
            "partition1", PartitionRange.BoundType.CLOSED
        )

        self.assertIsNotNone(down_closed_range.comparator())

        up_open_range = PartitionRange.up_to(
            "partition2", PartitionRange.BoundType.OPEN
        )

        self.assertIsNotNone(up_open_range.comparator())

    def test_all_partitions_comparator(self) -> None:
        self.assertIsNone(ALL_PARTITIONS.lower_bound_type())
        self.assertIsNone(ALL_PARTITIONS.upper_bound_type())

        self.assertIsNone(ALL_PARTITIONS.lower_partition_name())
        self.assertIsNone(ALL_PARTITIONS.upper_partition_name())

        self.assertIsNotNone(ALL_PARTITIONS.comparator())
