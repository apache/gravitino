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

import json as _json
import unittest

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.table_change import TableChange
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.requests.table_update_request import TableUpdateRequest


class TestTableUpdateRequest(unittest.TestCase):
    def test_rename_table_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.RenameTableRequest("")

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_rename_table_request_serialize(self) -> None:
        request = TableUpdateRequest.RenameTableRequest("newTable")
        json_str = _json.dumps(
            {
                "@type": "rename",
                "newName": "newTable",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_update_table_comment_request_serialize(self) -> None:
        request = TableUpdateRequest.UpdateTableCommentRequest("new comment")
        json_str = _json.dumps(
            {
                "@type": "updateComment",
                "newComment": "new comment",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_set_table_property_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.SetTablePropertyRequest("", "value")
        invalid_request2 = TableUpdateRequest.SetTablePropertyRequest("key", "")

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_set_table_property_request_serialize(self) -> None:
        request = TableUpdateRequest.SetTablePropertyRequest("key", "value1")
        json_str = _json.dumps(
            {
                "@type": "setProperty",
                "property": "key",
                "value": "value1",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_remove_table_property_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.RemoveTablePropertyRequest("")

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_remove_table_property_request_serialize(self) -> None:
        request = TableUpdateRequest.RemoveTablePropertyRequest("prop1")
        json_str = _json.dumps(
            {
                "@type": "removeProperty",
                "property": "prop1",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_add_table_column_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.AddTableColumnRequest(
            [],
            Types.StringType.get(),
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("hello")
            .build(),
            False,
            False,
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.AddTableColumnRequest(
            ["", "column"],
            Types.StringType.get(),
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("hello")
            .build(),
            False,
            False,
        )
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.AddTableColumnRequest(
            ["column"],
            None,
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("hello")
            .build(),
            False,
            False,
        )
        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_add_table_column_request_serialize(self) -> None:
        request = TableUpdateRequest.AddTableColumnRequest(
            ["column"],
            Types.StringType.get(),
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("hello")
            .build(),
            False,
            False,
        )
        json_str = _json.dumps(
            {
                "@type": "addColumn",
                "fieldName": ["column"],
                "type": "string",
                "comment": "comment",
                "position": {
                    "after": "afterColumn",
                },
                "defaultValue": {
                    "type": "literal",
                    "dataType": "string",
                    "value": "hello",
                },
                "nullable": False,
                "autoIncrement": False,
            }
        )
        self.assertEqual(json_str, request.to_json())

        request = TableUpdateRequest.AddTableColumnRequest(
            ["column"],
            Types.StringType.get(),
            None,
            TableChange.ColumnPosition.after("afterColumn"),
            LiteralDTO.builder()
            .with_data_type(Types.StringType.get())
            .with_value("hello")
            .build(),
            False,
            False,
        )
        json_str = _json.dumps(
            {
                "@type": "addColumn",
                "fieldName": ["column"],
                "type": "string",
                "comment": None,
                "position": {
                    "after": "afterColumn",
                },
                "defaultValue": {
                    "type": "literal",
                    "dataType": "string",
                    "value": "hello",
                },
                "nullable": False,
                "autoIncrement": False,
            }
        )

        self.assertEqual(json_str, request.to_json())

        request = TableUpdateRequest.AddTableColumnRequest(
            ["column"],
            Types.StringType.get(),
            "comment",
            TableChange.ColumnPosition.after("afterColumn"),
            None,
            False,
            False,
        )
        json_str = _json.dumps(
            {
                "@type": "addColumn",
                "fieldName": ["column"],
                "type": "string",
                "comment": "comment",
                "position": {
                    "after": "afterColumn",
                },
                "nullable": False,
                "autoIncrement": False,
            }
        )

        self.assertEqual(json_str, request.to_json())

    def test_rename_table_column_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.RenameTableColumnRequest([], "new_column")
        invalid_request2 = TableUpdateRequest.RenameTableColumnRequest(
            ["old_column"], ""
        )

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_rename_table_column_request_serialize(self) -> None:
        request = TableUpdateRequest.RenameTableColumnRequest(
            ["oldColumn"], "newColumn"
        )
        json_str = _json.dumps(
            {
                "@type": "renameColumn",
                "oldFieldName": ["oldColumn"],
                "newFieldName": "newColumn",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_update_table_column_default_value_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
            ["key"], Expression.EMPTY_EXPRESSION
        )
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
            [],
            LiteralDTO.builder()
            .with_data_type(Types.DateType.get())
            .with_value("2023-04-01")
            .build(),
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
            ["", "key"],
            LiteralDTO.builder()
            .with_data_type(Types.DateType.get())
            .with_value("2023-04-01")
            .build(),
        )
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
            ["key"],
            None,
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_update_table_column_default_value_request_serialize(self) -> None:
        request = TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
            ["key"],
            LiteralDTO.builder()
            .with_data_type(Types.DateType.get())
            .with_value("2023-04-01")
            .build(),
        )
        json_str = _json.dumps(
            {
                "@type": "updateColumnDefaultValue",
                "fieldName": ["key"],
                "newDefaultValue": {
                    "type": "literal",
                    "dataType": "date",
                    "value": "2023-04-01",
                },
            }
        )

        self.assertEqual(json_str, request.to_json())

    def test_update_table_column_comment_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.UpdateTableColumnCommentRequest(
            [], "new comment"
        )
        invalid_request2 = TableUpdateRequest.UpdateTableColumnCommentRequest(
            ["", "column2"], "new comment"
        )
        invalid_request3 = TableUpdateRequest.UpdateTableColumnCommentRequest(
            ["column"], ""
        )

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

        with self.assertRaises(ValueError):
            invalid_request3.validate()

    def test_update_table_column_comment_request_serialize(self) -> None:
        request = TableUpdateRequest.UpdateTableColumnCommentRequest(
            ["column"], "new comment"
        )
        json_str = _json.dumps(
            {
                "@type": "updateColumnComment",
                "fieldName": ["column"],
                "newComment": "new comment",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_update_table_column_nullability_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.UpdateTableColumnNullabilityRequest(
            [], True
        )
        invalid_request2 = TableUpdateRequest.UpdateTableColumnNullabilityRequest(
            ["", "column2"], False
        )

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_update_table_column_nullability_request_serialize(self) -> None:
        request = TableUpdateRequest.UpdateTableColumnNullabilityRequest(
            ["column"], False
        )
        json_str = _json.dumps(
            {
                "@type": "updateColumnNullability",
                "fieldName": ["column"],
                "nullable": False,
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_delete_column_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.DeleteTableColumnRequest([], False)
        invalid_request2 = TableUpdateRequest.DeleteTableColumnRequest(["", ""], True)

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_delete_column_request_serialize(self) -> None:
        request = TableUpdateRequest.DeleteTableColumnRequest(
            [
                "column1",
                "column2",
            ],
            True,
        )
        json_str = _json.dumps(
            {
                "@type": "deleteColumn",
                "fieldName": ["column1", "column2"],
                "ifExists": True,
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_add_table_index_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.AddTableIndexRequest(
            index_type=Index.IndexType.UNIQUE_KEY,
            name="",
            field_names=[["column1"]],
        )
        invalid_request2 = TableUpdateRequest.AddTableIndexRequest(
            index_type=Index.IndexType.UNIQUE_KEY,
            name="index_name",
            field_names=[],
        )

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_add_table_index_request_serialize(self) -> None:
        request = TableUpdateRequest.AddTableIndexRequest(
            index_type=Index.IndexType.PRIMARY_KEY,
            name=Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            field_names=[["column1"]],
        )
        json_str = _json.dumps(
            {
                "@type": "addTableIndex",
                "index": {
                    "indexType": Index.IndexType.PRIMARY_KEY,
                    "name": Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
                    "fieldNames": [["column1"]],
                },
            }
        )
        self.assertEqual(json_str, request.to_json())

    def test_delete_table_index_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.DeleteTableIndexRequest("", True)

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_delete_table_index_request_serialize(self) -> None:
        request = TableUpdateRequest.DeleteTableIndexRequest("uk_2", True)
        json_str = _json.dumps(
            {
                "@type": "deleteTableIndex",
                "name": "uk_2",
                "ifExists": True,
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_update_table_column_position_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.UpdateTableColumnPositionRequest(
            [], TableChange.ColumnPosition.first()
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnPositionRequest(
            ["column", ""], TableChange.ColumnPosition.first()
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnPositionRequest(
            ["column"], None
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_update_table_column_position_request_serialize(self) -> None:
        request1 = TableUpdateRequest.UpdateTableColumnPositionRequest(
            ["column"], TableChange.ColumnPosition.first()
        )
        json_str1 = _json.dumps(
            {
                "@type": "updateColumnPosition",
                "fieldName": ["column"],
                "newPosition": "first",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str1, request1.to_json())

        request2 = TableUpdateRequest.UpdateTableColumnPositionRequest(
            ["column"], TableChange.ColumnPosition.default_pos()
        )
        json_str2 = _json.dumps(
            {
                "@type": "updateColumnPosition",
                "fieldName": ["column"],
                "newPosition": "default",
            }
        )

        self.assertEqual(json_str2, request2.to_json())

        request3 = TableUpdateRequest.UpdateTableColumnPositionRequest(
            ["column"], TableChange.ColumnPosition.after("another_column")
        )
        json_str3 = _json.dumps(
            {
                "@type": "updateColumnPosition",
                "fieldName": ["column"],
                "newPosition": {"after": "another_column"},
            }
        )

        self.assertEqual(json_str3, request3.to_json())

    def test_update_column_auto_increment_request_validate(self) -> None:
        invalid_request1 = TableUpdateRequest.UpdateColumnAutoIncrementRequest([], True)
        invalid_request2 = TableUpdateRequest.UpdateColumnAutoIncrementRequest(
            ["", "column2"], False
        )

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_update_column_auto_increment_request_serialize(self) -> None:
        request = TableUpdateRequest.UpdateColumnAutoIncrementRequest(
            [
                "column1",
                "column2",
            ],
            False,
        )
        json_str = _json.dumps(
            {
                "@type": "updateColumnAutoIncrement",
                "fieldName": ["column1", "column2"],
                "autoIncrement": False,
            }
        )
        self.assertEqual(json_str, request.to_json())

    def test_update_table_column_type_request_validate(self) -> None:
        invalid_request = TableUpdateRequest.UpdateTableColumnTypeRequest(
            [], Types.StringType.get()
        )
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnTypeRequest(
            ["", "column"], Types.StringType.get()
        )
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TableUpdateRequest.UpdateTableColumnTypeRequest(
            ["column"], None
        )

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_update_table_column_type_request_serialize(self) -> None:
        request1 = TableUpdateRequest.UpdateTableColumnTypeRequest(
            ["column1"], Types.StringType.get()
        )

        json_str1 = _json.dumps(
            {
                "@type": "updateColumnType",
                "fieldName": ["column1"],
                "newType": "string",
            }
        )
        self.assertEqual(json_str1, request1.to_json())

        request2 = TableUpdateRequest.UpdateTableColumnTypeRequest(
            ["column2"],
            Types.StructType.of(
                Types.StructType.Field.not_null_field("id", Types.IntegerType.get()),
                Types.StructType.Field.not_null_field(
                    "name", Types.StringType.get(), "name field"
                ),
            ),
        )
        json_str2 = _json.dumps(
            {
                "@type": "updateColumnType",
                "fieldName": ["column2"],
                "newType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "id",
                            "type": "integer",
                            "nullable": False,
                        },
                        {
                            "name": "name",
                            "type": "string",
                            "nullable": False,
                            "comment": "name field",
                        },
                    ],
                },
            }
        )
        self.assertEqual(json_str2, request2.to_json())
