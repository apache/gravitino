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
from typing import cast

from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table_change import TableChange
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.requests.table_update_request import TableUpdateRequest
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.exceptions.base import IllegalArgumentException


class TestDTOConvertersToTableUpdateRequest(unittest.TestCase):
    def test_to_table_update_request_add_column_with_default_value(self):
        """Tests the conversion of an add column change with a default value to a TableUpdateRequest."""
        add_column = TableChange.add_column(
            field_name=["new_col"],
            data_type=Types.IntegerType.get(),
            comment="test column",
            nullable=False,
            auto_increment=True,
            default_value=Literals.integer_literal(10),
        )
        result = cast(
            TableUpdateRequest.AddTableColumnRequest,
            DTOConverters.to_table_update_request(add_column),
        )
        self.assertIsInstance(result, TableUpdateRequest.AddTableColumnRequest)
        add_column_request = cast(TableUpdateRequest.AddTableColumnRequest, result)
        self.assertListEqual(add_column_request.field_name, ["new_col"])
        self.assertEqual(add_column_request.data_type, Types.IntegerType.get())
        self.assertEqual(add_column_request.comment, "test column")
        self.assertIsNone(add_column_request.position)
        self.assertFalse(add_column_request.is_nullable)
        self.assertTrue(add_column_request.is_auto_increment)
        self.assertIsNotNone(add_column_request.default_value)
        self.assertIsInstance(add_column_request.default_value, LiteralDTO)
        default_value_dto = cast(LiteralDTO, add_column_request.default_value)
        self.assertEqual(default_value_dto.data_type(), Types.IntegerType.get())
        self.assertEqual(default_value_dto.value(), "10")

    def test_to_table_update_request_add_column_without_default_value(self):
        """Tests the conversion of an add column change without a default value to a TableUpdateRequest."""
        add_column = TableChange.add_column(
            field_name=["new_col"],
            data_type=Types.StringType.get(),
        )
        result = DTOConverters.to_table_update_request(add_column)
        self.assertIsInstance(result, TableUpdateRequest.AddTableColumnRequest)
        add_column_request = cast(TableUpdateRequest.AddTableColumnRequest, result)
        self.assertIsNone(add_column_request.default_value)

    def test_to_table_update_request_unsupported_change(self):
        """Tests that an unsupported table change raises an IllegalArgumentException."""
        with self.assertRaisesRegex(IllegalArgumentException, "Unknown change type"):
            DTOConverters.to_table_update_request("invalid_change")

    def test_to_table_update_request_rename_column(self):
        """Tests the conversion of a rename column change to a TableUpdateRequest."""
        rename_column = TableChange.rename_column(
            field_name=["old_col"],
            new_name="new_col",
        )
        result = DTOConverters.to_table_update_request(rename_column)
        self.assertIsInstance(result, TableUpdateRequest.RenameTableColumnRequest)
        rename_request = cast(TableUpdateRequest.RenameTableColumnRequest, result)
        self.assertListEqual(
            rename_request._old_field_name,  # pylint: disable=protected-access
            ["old_col"],
        )
        self.assertEqual(
            rename_request._new_field_name,  # pylint: disable=protected-access
            "new_col",
        )

    def test_to_table_update_request_update_column_default_value_full(self):
        """Tests the conversion of an update column default value change to a TableUpdateRequest."""
        literals = [
            Literals.integer_literal(42),
            Literals.float_literal(3.14),
            Literals.string_literal("newDefaultValue"),
            Literals.NULL,
        ]
        for literal in literals:
            update_default = TableChange.update_column_default_value(
                field_name=["col"],
                new_default_value=literal,
            )
            result = DTOConverters.to_table_update_request(update_default)
            self.assertIsInstance(
                result, TableUpdateRequest.UpdateTableColumnDefaultValueRequest
            )
            update_request = cast(
                TableUpdateRequest.UpdateTableColumnDefaultValueRequest, result
            )
            self.assertListEqual(update_request.field_name, ["col"])
            self.assertIsInstance(update_request.new_default_value, LiteralDTO)
            integer_literal_dto = cast(LiteralDTO, update_request.new_default_value)
            self.assertEqual(integer_literal_dto.data_type(), literal.data_type())
            self.assertEqual(
                integer_literal_dto.value(),
                "NULL" if literal.value() is None else str(literal.value()),
            )

    def test_to_table_update_request_update_column_type(self):
        """Tests the conversion of an update column type change to a TableUpdateRequest."""
        column_types = [
            Types.IntegerType.get(),
            Types.FloatType.get(),
            Types.StringType.get(),
            Types.BooleanType.get(),
            Types.NullType.get(),
        ]
        for new_column_type in column_types:
            update_type = TableChange.update_column_type(
                field_name=["col"],
                new_data_type=new_column_type,
            )
            result = DTOConverters.to_table_update_request(update_type)
            self.assertIsInstance(
                result, TableUpdateRequest.UpdateTableColumnTypeRequest
            )
            update_request = cast(
                TableUpdateRequest.UpdateTableColumnTypeRequest, result
            )
            self.assertListEqual(update_request.field_name, ["col"])
            self.assertEqual(update_request.new_type, new_column_type)

    def test_to_table_update_request_update_column_comment(self):
        """Tests the conversion of an update column comment change to a TableUpdateRequest."""
        update_comment = TableChange.update_column_comment(
            field_name=["col"],
            new_comment="new comment",
        )
        result = DTOConverters.to_table_update_request(update_comment)
        self.assertIsInstance(
            result, TableUpdateRequest.UpdateTableColumnCommentRequest
        )
        update_request = cast(
            TableUpdateRequest.UpdateTableColumnCommentRequest, result
        )
        self.assertListEqual(update_request.field_name, ["col"])
        self.assertEqual(update_request.new_comment, "new comment")

    def test_to_table_update_request_update_column_position(self):
        """Tests the conversion of an update column position change with a ColumnPosition."""
        positions = [
            TableChange.ColumnPosition.first(),
            TableChange.ColumnPosition.default_pos(),
            TableChange.ColumnPosition.after("ref_col"),
        ]
        for position in positions:
            update_position = TableChange.update_column_position(
                field_name=["col"],
                new_position=position,
            )
            result = DTOConverters.to_table_update_request(update_position)
            self.assertIsInstance(
                result, TableUpdateRequest.UpdateTableColumnPositionRequest
            )
            update_request = cast(
                TableUpdateRequest.UpdateTableColumnPositionRequest, result
            )
            self.assertListEqual(update_request.field_name, ["col"])
            self.assertEqual(str(update_request.new_position), str(position))

    def test_to_table_update_request_delete_column(self):
        """Tests the conversion of a delete column change to a TableUpdateRequest."""

        for exists in (True, False):
            delete_column = TableChange.delete_column(
                field_name=["col"],
                if_exists=exists,
            )
            result = DTOConverters.to_table_update_request(delete_column)
            self.assertIsInstance(result, TableUpdateRequest.DeleteTableColumnRequest)
            delete_request = cast(TableUpdateRequest.DeleteTableColumnRequest, result)
            self.assertListEqual(delete_request.field_name, ["col"])
            self.assertEqual(delete_request.if_exists, exists)

    def test_to_table_update_request_update_column_nullability(self):
        """Tests the conversion of an update column nullability change to a TableUpdateRequest."""
        for nullable in (True, False):
            update_nullability = TableChange.update_column_nullability(
                field_name=["col"],
                nullable=nullable,
            )
            result = DTOConverters.to_table_update_request(update_nullability)
            self.assertIsInstance(
                result, TableUpdateRequest.UpdateTableColumnNullabilityRequest
            )
            update_request = cast(
                TableUpdateRequest.UpdateTableColumnNullabilityRequest, result
            )
            self.assertListEqual(update_request.field_name, ["col"])
            self.assertEqual(update_request.nullable, nullable)

    def test_to_table_update_request_update_column_auto_increment(self):
        """Tests the conversion of an update column auto increment change to a TableUpdateRequest."""
        for auto_increment in (True, False):
            update_auto_increment = TableChange.update_column_auto_increment(
                field_name=["col"],
                auto_increment=auto_increment,
            )
            result = DTOConverters.to_table_update_request(update_auto_increment)
            self.assertIsInstance(
                result, TableUpdateRequest.UpdateColumnAutoIncrementRequest
            )
            update_request = cast(
                TableUpdateRequest.UpdateColumnAutoIncrementRequest, result
            )
            self.assertListEqual(update_request.field_name, ["col"])
            self.assertEqual(update_request.auto_increment, auto_increment)

    def test_to_table_update_request_rename_table(self):
        """Tests the conversion of a rename table change to a TableUpdateRequest."""
        rename_table = TableChange.rename("new_table_name")
        result = DTOConverters.to_table_update_request(rename_table)
        self.assertIsInstance(result, TableUpdateRequest.RenameTableRequest)
        rename_request = cast(TableUpdateRequest.RenameTableRequest, result)
        self.assertEqual(rename_request.new_name, "new_table_name")
        self.assertIsNone(rename_request.new_schema_name)

    def test_to_table_update_request_update_comment(self):
        """Tests the conversion of an update comment change to a TableUpdateRequest."""
        update_comment = TableChange.update_comment("new comment")
        result = DTOConverters.to_table_update_request(update_comment)
        self.assertIsInstance(result, TableUpdateRequest.UpdateTableCommentRequest)
        comment_request = cast(TableUpdateRequest.UpdateTableCommentRequest, result)
        self.assertEqual(comment_request.new_comment, "new comment")

    def test_to_table_update_request_set_property(self):
        """Tests the conversion of a set property change to a TableUpdateRequest."""
        set_property = TableChange.set_property("key", "value")
        result = DTOConverters.to_table_update_request(set_property)
        self.assertIsInstance(result, TableUpdateRequest.SetTablePropertyRequest)
        property_request = cast(TableUpdateRequest.SetTablePropertyRequest, result)
        self.assertEqual(property_request.prop, "key")
        self.assertEqual(property_request.prop_value, "value")

    def test_to_table_update_request_remove_property(self):
        """Tests the conversion of a remove property change to a TableUpdateRequest."""
        remove_property = TableChange.remove_property("key")
        result = DTOConverters.to_table_update_request(remove_property)
        self.assertIsInstance(result, TableUpdateRequest.RemoveTablePropertyRequest)
        property_request = cast(TableUpdateRequest.RemoveTablePropertyRequest, result)
        self.assertEqual(property_request.property, "key")

    def test_to_table_update_request_add_index(self):
        """Tests the conversion of an add index change to a TableUpdateRequest."""

        add_index = TableChange.add_index(
            index_type=Index.IndexType.PRIMARY_KEY,
            name="pk_index",
            field_names=[["id"]],
        )
        result = DTOConverters.to_table_update_request(add_index)
        self.assertIsInstance(result, TableUpdateRequest.AddTableIndexRequest)
        index_request = cast(TableUpdateRequest.AddTableIndexRequest, result)
        self.assertEqual(index_request.index.type(), Index.IndexType.PRIMARY_KEY)
        self.assertEqual(index_request.index.name(), "pk_index")

    def test_to_table_update_request_delete_index(self):
        """Tests the conversion of a delete index change to a TableUpdateRequest."""
        delete_index = TableChange.delete_index("idx_name", if_exists=False)
        result = DTOConverters.to_table_update_request(delete_index)
        self.assertIsInstance(result, TableUpdateRequest.DeleteTableIndexRequest)
        index_request = cast(TableUpdateRequest.DeleteTableIndexRequest, result)
        self.assertEqual(index_request.name, "idx_name")
        self.assertFalse(index_request.if_exists)
