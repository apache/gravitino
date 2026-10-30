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
from unittest.mock import Mock

from gravitino.api.rel.table import Table
from gravitino.api.stats.partition_range import PartitionRange
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)


class TestPartitionStatisticsModels(unittest.TestCase):
    def test_range_factories(self):
        bound = PartitionRange.BoundType
        lower = PartitionRange.down_to("p1", bound.CLOSED)
        self.assertEqual("p1", lower.lower_partition_name)
        self.assertIsNone(lower.upper_partition_name)
        upper = PartitionRange.up_to("p3", bound.OPEN)
        self.assertEqual("p3", upper.upper_partition_name)
        self.assertIsNone(upper.lower_partition_name)
        self.assertEqual(
            PartitionRange("p1", bound.OPEN, "p3", bound.CLOSED),
            PartitionRange.between("p1", bound.OPEN, "p3", bound.CLOSED),
        )

    def test_invalid_range_pairs(self):
        for kwargs in [
            {"lower_partition_name": "p1"},
            {"upper_bound_type": PartitionRange.BoundType.OPEN},
            {
                "lower_partition_name": "",
                "lower_bound_type": PartitionRange.BoundType.OPEN,
            },
            {
                "upper_partition_name": " ",
                "upper_bound_type": PartitionRange.BoundType.CLOSED,
            },
            {"lower_partition_name": "p", "lower_bound_type": "CLOSED"},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(
                IllegalArgumentException
            ):
                PartitionRange(**kwargs)

    def test_factories_require_their_endpoints(self):
        for factory, args in [
            (PartitionRange.up_to, (None, None)),
            (PartitionRange.down_to, (None, None)),
            (PartitionRange.between, (None, None, None, None)),
        ]:
            with self.subTest(factory=factory), self.assertRaises(
                IllegalArgumentException
            ):
                factory(*args)

    def test_default_table_entry_is_unsupported(self):
        with self.assertRaises(UnsupportedOperationException):
            Table.supports_partition_statistics(Mock(spec=Table))
