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

from gravitino.api.expressions.sorts.null_ordering import NullOrdering
from gravitino.api.expressions.sorts.sort_direction import SortDirection
from gravitino.api.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.expressions.field_reference_dto import FieldReferenceDTO
from gravitino.dto.rel.sort_order_dto import SortOrderDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestSortOrderDTO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.field_ref_dto = (
            FieldReferenceDTO.builder().with_field_name(["field1"]).build()
        )
        cls.columns = [
            ColumnDTO.builder()
            .with_name("field1")
            .with_data_type(Types.StringType.get())
            .build(),
            ColumnDTO.builder()
            .with_name("field2")
            .with_data_type(Types.StringType.get())
            .build(),
        ]
        cls.sort_order_dto = SortOrderDTO(
            cls.field_ref_dto,
            SortDirection.ASCENDING,
            NullOrdering.NULLS_FIRST,
        )

    def test_sort_dto_class_vars(self):
        self.assertEqual(SortOrderDTO.EMPTY_SORT, [])
        self.assertEqual(SortOrderDTO.EMPTY_EXPRESSION, [])
        self.assertEqual(SortOrderDTO.EMPTY_NAMED_REFERENCE, [])

    def test_sort_order_dto_init(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "expression cannot be null"
        ):
            SortOrderDTO(None, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)

        self.sort_order_dto.validate(self.columns)
        self.assertTrue(self.sort_order_dto.sort_term() == self.field_ref_dto)
        self.assertIs(self.sort_order_dto.direction(), SortDirection.ASCENDING)
        self.assertIs(self.sort_order_dto.null_ordering(), NullOrdering.NULLS_FIRST)
        self.assertEqual(
            self.sort_order_dto.expression(), self.sort_order_dto.sort_term()
        )
