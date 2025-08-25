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

from gravitino.api.expressions.indexes.index import Index
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestIndexDTO(unittest.TestCase):
    def test_index_dto_invalid_init(self):
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Index type cannot be null",
            IndexDTO,
            None,
            "invalid_index_type",
            [["field1", "field2"]],
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "The index must be set with corresponding column names",
            IndexDTO,
            Index.IndexType.PRIMARY_KEY,
            "invalid_field_names",
            None,
        )
        self.assertRaisesRegex(
            IllegalArgumentException,
            "The index must be set with corresponding column names",
            IndexDTO,
            Index.IndexType.PRIMARY_KEY,
            "invalid_field_names",
            [],
        )

    def test_index_dto(self):
        names = (None, "", "index_name")
        field_names = [["col1"], ["col2"]]
        dto_dict = {}
        index_dto = None
        for idx, name in enumerate(names):
            index_dto = IndexDTO(Index.IndexType.PRIMARY_KEY, name, field_names)
            dto_dict[index_dto] = idx

            self.assertIs(index_dto.type(), Index.IndexType.PRIMARY_KEY)
            self.assertEqual(index_dto.name(), name)
            self.assertListEqual(index_dto.field_names(), field_names)

        slack_dto = IndexDTO(Index.IndexType.PRIMARY_KEY, "index_name", field_names)
        self.assertTrue(index_dto == slack_dto)
        self.assertFalse(index_dto == "index_dto")
        self.assertEqual(dto_dict[slack_dto], len(names) - 1)

        dto_dict[slack_dto] = len(names)
        self.assertEqual(len(dto_dict), len(names))
        self.assertEqual(dto_dict[slack_dto], len(names))
