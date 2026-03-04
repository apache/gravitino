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

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.types.types import Types
from gravitino.client.generic_column import GenericColumn
from gravitino.utils import HTTPClient


class TestGenericColumn(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._rest_client = HTTPClient("http://localhost:8090")
        cls._metalake_name = "metalake_demo"
        cls._catalog_name = "relational_catalog"
        cls._schema_name = "test_schema"
        cls._table_name = "test_table"
        cls._column_name = "test_column"
        cls._column_data_type = Types.StringType.get()
        cls._column_default_value = Literals.string_literal(value="test")
        cls._column = Column.of(
            name=TestGenericColumn._column_name,
            data_type=TestGenericColumn._column_data_type,
            nullable=False,
            default_value=TestGenericColumn._column_default_value,
        )
        cls._generic_column = GenericColumn(
            column=TestGenericColumn._column,
            rest_client=TestGenericColumn._rest_client,
            metalake=TestGenericColumn._metalake_name,
            catalog=TestGenericColumn._catalog_name,
            schema=TestGenericColumn._schema_name,
            table=TestGenericColumn._table_name,
        )

    def test_generic_column(self):
        self.assertEqual(
            TestGenericColumn._generic_column.name(), TestGenericColumn._column.name()
        )
        self.assertEqual(
            TestGenericColumn._generic_column.data_type(),
            TestGenericColumn._column.data_type(),
        )
        self.assertEqual(
            TestGenericColumn._generic_column.nullable(),
            TestGenericColumn._column.nullable(),
        )
        self.assertEqual(
            TestGenericColumn._generic_column.default_value(),
            TestGenericColumn._column.default_value(),
        )
        self.assertEqual(
            TestGenericColumn._generic_column.auto_increment(),
            TestGenericColumn._column.auto_increment(),
        )
        self.assertEqual(
            TestGenericColumn._generic_column.comment(),
            TestGenericColumn._column.comment(),
        )

    def test_generic_column_hash(self):
        another_column = Column.of(
            name="another_column",
            data_type=TestGenericColumn._column_data_type,
            nullable=False,
            default_value=TestGenericColumn._column_default_value,
        )
        another_generic_column = GenericColumn(
            column=another_column,
            rest_client=TestGenericColumn._rest_client,
            metalake=TestGenericColumn._metalake_name,
            catalog=TestGenericColumn._catalog_name,
            schema=TestGenericColumn._schema_name,
            table=TestGenericColumn._table_name,
        )
        self.assertNotEqual(
            hash(TestGenericColumn._generic_column),
            hash(another_generic_column),
        )

    def test_generic_column_equality(self):
        another_column = Column.of(
            name=TestGenericColumn._column_name,
            data_type=TestGenericColumn._column_data_type,
            nullable=False,
            default_value=TestGenericColumn._column_default_value,
        )
        another_generic_column = GenericColumn(
            column=another_column,
            rest_client=TestGenericColumn._rest_client,
            metalake=TestGenericColumn._metalake_name,
            catalog=TestGenericColumn._catalog_name,
            schema=TestGenericColumn._schema_name,
            table=TestGenericColumn._table_name,
        )
        self.assertEqual(TestGenericColumn._generic_column, another_generic_column)
        self.assertFalse(another_generic_column == "invalid_generic_column")
