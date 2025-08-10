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

from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.bucket_partitioning_dto import BucketPartitioningDTO
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.exceptions.base import IllegalArgumentException


class TestBucketPartitioningDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.columns = [
            ColumnDTO.builder()
            .with_name("field1")
            .with_data_type(Types.StringType.get())
            .with_comment("test field 1")
            .build(),
            ColumnDTO.builder()
            .with_name("field2")
            .with_data_type(Types.IntegerType.get())
            .with_comment("test field 2")
            .build(),
        ]
        cls.dto = BucketPartitioningDTO(10, ["field1"], ["field2"])

    def test_bucket_partitioning_dto(self):
        self.dto.validate(self.columns)
        self.assertEqual(10, self.dto.num_buckets())
        self.assertEqual([["field1"], ["field2"]], self.dto.field_names())
        self.assertEqual(Partitioning.Strategy.BUCKET, self.dto.strategy())
        self.assertEqual("bucket", self.dto.name())

    def test_validate_non_existing_field(self):
        dto = BucketPartitioningDTO(6, ["nonexistent"])
        with self.assertRaises(IllegalArgumentException):
            dto.validate(self.columns)

    def test_arguments(self):
        arguments = self.dto.arguments()
        self.assertIsNotNone(arguments)
        self.assertIsInstance(arguments, list)
        self.assertEqual(
            arguments,
            [
                Literals.integer_literal(10),
                NamedReference.field(field_name=["field1"]),
                NamedReference.field(field_name=["field2"]),
            ],
        )
