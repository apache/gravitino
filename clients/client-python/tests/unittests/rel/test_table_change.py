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
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.table_change import TableChange
from gravitino.api.rel.types.types import Types


class TestTableChange(unittest.TestCase):
    def test_table_change_rename(self):
        rename1, rename2 = (
            TableChange.rename(f"New table name {i + 1}") for i in range(2)
        )
        rename3 = TableChange.rename("New table name 1")
        self.assertEqual(rename1.get_new_name(), "New table name 1")
        self.assertEqual(str(rename1), f"RENAMETABLE {rename1.get_new_name()}")
        self.assertFalse(rename1 == rename2)
        self.assertFalse(rename1 == "invalid_rename")
        self.assertTrue(rename1 == rename3)
        self.assertTrue(hash(rename1) == hash(rename3))
        self.assertFalse(hash(rename1) == hash(rename2))

    def test_table_change_update_comment(self):
        new_comment1, new_comment2 = (
            TableChange.update_comment(f"New comment {i + 1}") for i in range(2)
        )
        new_comment3 = TableChange.update_comment("New comment 1")
        self.assertEqual(new_comment1.get_new_comment(), "New comment 1")
        self.assertEqual(
            str(new_comment1), f"UPDATECOMMENT {new_comment1.get_new_comment()}"
        )
        self.assertFalse(new_comment1 == new_comment2)
        self.assertFalse(new_comment1 == "invalid_update_comment")
        self.assertTrue(new_comment1 == new_comment3)
        self.assertTrue(hash(new_comment1) == hash(new_comment3))
        self.assertFalse(hash(new_comment1) == hash(new_comment2))

    def test_table_change_set_property(self):
        new_property1, new_property2 = (
            TableChange.set_property(f"new_property_{i + 1}", str(i + 1))
            for i in range(2)
        )
        new_property3 = TableChange.set_property("new_property_1", "1")
        self.assertEqual(new_property1.get_property(), "new_property_1")
        self.assertEqual(new_property1.get_value(), "1")
        self.assertEqual(
            str(new_property1),
            f"SETPROPERTY {new_property1.get_property()} {new_property1.get_value()}",
        )
        self.assertFalse(new_property1 == new_property2)
        self.assertFalse(new_property1 == "invalid_set_property")
        self.assertTrue(new_property1 == new_property3)
        self.assertTrue(hash(new_property1) == hash(new_property3))
        self.assertFalse(hash(new_property1) == hash(new_property2))

    def test_table_change_remove_property(self):
        property1, property2 = (
            TableChange.remove_property(f"property_{i + 1}") for i in range(2)
        )
        property3 = TableChange.remove_property("property_1")
        self.assertEqual(property1.get_property(), "property_1")
        self.assertEqual(str(property1), f"REMOVEPROPERTY {property1.get_property()}")
        self.assertFalse(property1 == property2)
        self.assertFalse(property1 == "invalid_remove_property")
        self.assertTrue(property1 == property3)
        self.assertTrue(hash(property1) == hash(property3))
        self.assertFalse(hash(property1) == hash(property2))

    def test_column_position(self):
        first = TableChange.ColumnPosition.first()
        after = TableChange.ColumnPosition.after("column")
        default_pos = TableChange.ColumnPosition.default_pos()

        self.assertIsInstance(first, TableChange.ColumnPosition)
        self.assertIsInstance(after, TableChange.ColumnPosition)
        self.assertIsInstance(default_pos, TableChange.ColumnPosition)
        self.assertEqual(after.get_column(), "column")
        self.assertEqual(str(first), "FIRST")
        self.assertEqual(str(after), "AFTER column")
        self.assertEqual(str(default_pos), "DEFAULT")

    def test_column_position_equal_and_hash(self):
        first = TableChange.ColumnPosition.first()
        after = TableChange.ColumnPosition.after("column")
        default_pos = TableChange.ColumnPosition.default_pos()

        self.assertFalse(first == after)
        self.assertTrue(first == TableChange.ColumnPosition.first())
        self.assertFalse(after == default_pos)
        self.assertTrue(after == TableChange.ColumnPosition.after("column"))
        self.assertTrue(hash(after) == hash(TableChange.ColumnPosition.after("column")))
        self.assertFalse(
            hash(after) == hash(TableChange.ColumnPosition.after("aonther column"))
        )
        self.assertFalse(default_pos == first)
        self.assertTrue(default_pos == TableChange.ColumnPosition.default_pos())

    def test_add_column(self):
        add_col_mandatory = TableChange.add_column(["col1"], Types.StringType.get())
        self.assertListEqual(add_col_mandatory.get_field_name(), ["col1"])
        self.assertListEqual(add_col_mandatory.field_name(), ["col1"])
        self.assertEqual(add_col_mandatory.get_data_type(), Types.StringType.get())
        self.assertIsNone(add_col_mandatory.get_comment())
        self.assertIsNone(add_col_mandatory.get_position())
        self.assertTrue(add_col_mandatory.is_nullable())
        self.assertFalse(add_col_mandatory.is_auto_increment())
        self.assertEqual(
            add_col_mandatory.get_default_value(), Column.DEFAULT_VALUE_NOT_SET
        )

    def test_add_column_with_position(self):
        field_name = ["Full Name", "First Name"]
        data_type = Types.StringType.get()
        comment = "First or given name"
        position = TableChange.ColumnPosition.after("Address")
        add_column = TableChange.add_column(field_name, data_type, comment, position)
        self.assertListEqual(add_column.get_field_name(), field_name)
        self.assertEqual(add_column.get_data_type(), data_type)
        self.assertEqual(add_column.get_comment(), comment)
        self.assertEqual(add_column.get_position(), position)
        self.assertTrue(add_column.is_nullable())
        self.assertFalse(add_column.is_auto_increment())
        self.assertEqual(add_column.get_default_value(), Column.DEFAULT_VALUE_NOT_SET)

    def test_add_column_with_null_comment_and_position(self):
        field_name = ["Middle Name"]
        data_type = Types.StringType.get()
        add_column = TableChange.add_column(field_name, data_type, None, None)
        self.assertListEqual(add_column.get_field_name(), field_name)
        self.assertEqual(add_column.get_data_type(), data_type)
        self.assertIsNone(add_column.get_comment())
        self.assertIsNone(add_column.get_position())
        self.assertTrue(add_column.is_nullable())
        self.assertFalse(add_column.is_auto_increment())
        self.assertEqual(add_column.get_default_value(), Column.DEFAULT_VALUE_NOT_SET)

    def test_add_column_equal_and_hash(self):
        field_name = ["Name"]
        another_field_name = ["First Name"]
        data_type = Types.StringType.get()
        comment = "Person name"
        add_columns = [
            TableChange.add_column(field_name, data_type, comment) for _ in range(2)
        ]
        add_column_dict = {add_columns[i]: i for i in range(2)}

        self.assertTrue(add_columns[0] == add_columns[1])
        self.assertTrue(add_columns[1] == add_columns[0])
        self.assertFalse(add_columns[0] == "invalid_add_column")
        self.assertEqual(len(add_column_dict), 1)
        self.assertEqual(add_column_dict[add_columns[0]], 1)

        another_add_column = TableChange.add_column(
            another_field_name, data_type, comment
        )
        add_column_dict = {add_columns[0]: 0, another_add_column: 1}
        self.assertFalse(add_columns[0] == another_add_column)
        self.assertFalse(another_add_column == add_columns[0])
        self.assertEqual(len(add_column_dict), 2)

    def test_rename_column(self):
        field_name = ["Last Name"]
        new_name = "Family Name"
        rename_column = TableChange.rename_column(field_name, new_name)
        self.assertListEqual(rename_column.get_field_name(), field_name)
        self.assertListEqual(rename_column.field_name(), field_name)
        self.assertEqual(rename_column.get_new_name(), new_name)

    def test_rename_nested_column(self):
        field_name = ["Name", "First Name"]
        new_name = "Name.first"
        rename_column = TableChange.rename_column(field_name, new_name)
        self.assertListEqual(rename_column.field_name(), field_name)
        self.assertEqual(rename_column.get_new_name(), new_name)

    def test_column_rename_equal_and_hash(self):
        field_name = ["Name"]
        another_field_name = ["First Name"]
        new_name = "Family Name"
        rename_columns = [
            TableChange.rename_column(field_name, new_name) for _ in range(2)
        ]
        rename_column_dict = {rename_columns[i]: i for i in range(2)}

        self.assertTrue(rename_columns[0] == rename_columns[1])
        self.assertTrue(rename_columns[1] == rename_columns[0])
        self.assertFalse(rename_columns[0] == "invalid_rename_column")
        self.assertEqual(len(rename_column_dict), 1)
        self.assertEqual(rename_column_dict[rename_columns[0]], 1)

        another_rename_column = TableChange.rename_column(another_field_name, new_name)
        rename_column_dict = {rename_columns[0]: 0, another_rename_column: 1}
        self.assertFalse(rename_columns[0] == another_rename_column)
        self.assertFalse(another_rename_column == rename_columns[0])
        self.assertEqual(len(rename_column_dict), 2)

    def test_update_column_default_value(self):
        field_name_data = [["existing_column"], ["nested", "existing_column"]]
        new_default_value = Literals.of("Default Value", Types.VarCharType.of(255))

        for field_name in field_name_data:
            update_column_default_value = TableChange.update_column_default_value(
                field_name, new_default_value
            )
            self.assertListEqual(update_column_default_value.field_name(), field_name)
            self.assertEqual(
                update_column_default_value.get_new_default_value(), new_default_value
            )

        update_column_default_value = TableChange.update_column_default_value(
            field_name_data[0], Column.DEFAULT_VALUE_NOT_SET
        )
        self.assertListEqual(
            update_column_default_value.field_name(), field_name_data[0]
        )
        self.assertEqual(
            update_column_default_value.get_new_default_value(),
            Column.DEFAULT_VALUE_NOT_SET,
        )

    def test_update_column_default_value_equal_and_hash(self):
        field_name = ["existing_column"]
        new_default_value = Literals.of("Default Value", Types.VarCharType.of(255))
        update_column_default_values = [
            TableChange.update_column_default_value(field_name, new_default_value)
            for _ in range(2)
        ]
        update_column_default_value_dict = {
            update_column_default_values[i]: i for i in range(2)
        }

        self.assertTrue(
            update_column_default_values[0] == update_column_default_values[1]
        )
        self.assertTrue(
            update_column_default_values[1] == update_column_default_values[0]
        )
        self.assertFalse(
            update_column_default_values[0] == "invalid_update_column_default_value"
        )
        self.assertEqual(len(update_column_default_value_dict), 1)
        self.assertEqual(
            update_column_default_value_dict[update_column_default_values[0]], 1
        )

        update_column_default_values = [
            TableChange.update_column_default_value(
                field_name, Column.DEFAULT_VALUE_NOT_SET
            )
            for _ in range(2)
        ]

        update_column_default_value_dict = {
            update_column_default_values[i]: i for i in range(2)
        }

        self.assertTrue(
            update_column_default_values[0] == update_column_default_values[1]
        )
        self.assertTrue(
            update_column_default_values[1] == update_column_default_values[0]
        )
        self.assertEqual(len(update_column_default_value_dict), 1)
        self.assertEqual(
            update_column_default_value_dict[update_column_default_values[0]], 1
        )

    def test_update_column_type(self):
        field_name_data = [["existing_column"], ["nested", "existing_column"]]
        data_type = Types.StringType.get()
        for field_name in field_name_data:
            update_column_type = TableChange.update_column_type(field_name, data_type)
            self.assertListEqual(update_column_type.field_name(), field_name)
            self.assertListEqual(update_column_type.get_field_name(), field_name)
            self.assertTrue(update_column_type.get_new_data_type() == data_type)

    def test_update_column_type_equal_and_hash(self):
        field_name = ["First Name"]
        data_type = Types.StringType.get()
        update_column_types = [
            TableChange.update_column_type(field_name, data_type) for _ in range(2)
        ]
        update_column_type_dict = {update_column_types[i]: i for i in range(2)}

        self.assertTrue(update_column_types[0] == update_column_types[1])
        self.assertTrue(update_column_types[1] == update_column_types[0])
        self.assertFalse(update_column_types[0] == "invalid_update_column_type")
        self.assertEqual(len(update_column_type_dict), 1)
        self.assertEqual(update_column_type_dict[update_column_types[0]], 1)

        another_update_column_type = TableChange.update_column_type(
            ["Last Name"], data_type
        )
        update_column_type_dict = {
            update_column_types[0]: 0,
            another_update_column_type: 1,
        }
        self.assertFalse(update_column_types[0] == another_update_column_type)
        self.assertFalse(another_update_column_type == update_column_types[0])
        self.assertEqual(len(update_column_type_dict), 2)

    def test_update_column_comment(self):
        field_name_data = [["First Name"], ["nested", "Last Name"]]
        comment_data = ["First or given name", "Last or family name"]
        for field_name, comment in zip(field_name_data, comment_data):
            update_column_comment = TableChange.update_column_comment(
                field_name, comment
            )
            self.assertListEqual(update_column_comment.field_name(), field_name)
            self.assertListEqual(update_column_comment.get_field_name(), field_name)
            self.assertEqual(update_column_comment.get_new_comment(), comment)

    def test_update_column_comment_equal_and_hash(self):
        field_name = ["First Name"]
        comment = "First or given name"
        update_column_comments = [
            TableChange.update_column_comment(field_name, comment) for _ in range(2)
        ]
        update_column_comment_dict = {update_column_comments[i]: i for i in range(2)}
        self.assertTrue(update_column_comments[0] == update_column_comments[1])
        self.assertTrue(update_column_comments[1] == update_column_comments[0])
        self.assertEqual(len(update_column_comment_dict), 1)

        another_update_column_comment = TableChange.update_column_comment(
            ["Last Name"], "Last or family name"
        )
        update_column_comment_dict = {
            update_column_comments[0]: 0,
            another_update_column_comment: 1,
        }
        self.assertFalse(update_column_comments[0] == another_update_column_comment)
        self.assertFalse(another_update_column_comment == update_column_comments[0])
        self.assertFalse(update_column_comments[0] == "invalid_update_column_comment")
        self.assertEqual(len(update_column_comment_dict), 2)

    def test_update_column_position(self):
        field_name_data = [["First Name"], ["nested", "Last Name"]]
        position_data = [
            TableChange.ColumnPosition.first(),
            TableChange.ColumnPosition.after("First Name"),
        ]
        for field_name, position in zip(field_name_data, position_data):
            update_column_position = TableChange.update_column_position(
                field_name, position
            )
            self.assertListEqual(update_column_position.field_name(), field_name)
            self.assertListEqual(update_column_position.get_field_name(), field_name)
            self.assertEqual(update_column_position.get_position(), position)

    def test_update_column_position_equal_and_hash(self):
        field_name = ["First Name"]
        position = TableChange.ColumnPosition.first()
        update_column_positions = [
            TableChange.update_column_position(field_name, position) for _ in range(2)
        ]
        update_column_position_dict = {update_column_positions[i]: i for i in range(2)}
        self.assertTrue(update_column_positions[0] == update_column_positions[1])
        self.assertTrue(update_column_positions[1] == update_column_positions[0])
        self.assertEqual(len(update_column_position_dict), 1)

        another_update_column_position = TableChange.update_column_position(
            ["Last Name"], TableChange.ColumnPosition.after("First Name")
        )
        update_column_position_dict = {
            update_column_positions[0]: 0,
            another_update_column_position: 1,
        }
        self.assertFalse(update_column_positions[0] == another_update_column_position)
        self.assertFalse(another_update_column_position == update_column_positions[0])
        self.assertFalse(update_column_positions[0] == "invalid_update_column_position")
        self.assertEqual(len(update_column_position_dict), 2)

    def test_delete_column(self):
        field_name_data = [["existing_column"], ["nested", "existing_column"]]
        if_exists_data = [True, False]
        for field_name, if_exists in zip(field_name_data, if_exists_data):
            delete_column = TableChange.delete_column(field_name, if_exists)
            self.assertListEqual(delete_column.field_name(), field_name)
            self.assertListEqual(delete_column.get_field_name(), field_name)
            self.assertEqual(delete_column.get_if_exists(), if_exists)

    def test_delete_column_equal_and_hash(self):
        field_name = ["Column A"]
        if_exists = True
        delete_columns = [
            TableChange.delete_column(field_name, if_exists) for _ in range(2)
        ]
        delete_column_dict = {delete_columns[i]: i for i in range(2)}
        self.assertTrue(delete_columns[0] == delete_columns[1])
        self.assertTrue(delete_columns[1] == delete_columns[0])
        self.assertEqual(len(delete_column_dict), 1)

        another_delete_column = TableChange.delete_column(["Column B"], if_exists)
        delete_column_dict = {delete_columns[0]: 0, another_delete_column: 1}
        self.assertFalse(delete_columns[0] == another_delete_column)
        self.assertFalse(another_delete_column == delete_columns[0])
        self.assertFalse(delete_columns[0] == "invalid_delete_column")
        self.assertEqual(len(delete_column_dict), 2)

    def test_update_column_nullability(self):
        field_name_data = [["existing_column"], ["nested", "existing_column"]]
        nullable_data = [True, False]
        for field_name, nullable in zip(field_name_data, nullable_data):
            update_column_nullability = TableChange.update_column_nullability(
                field_name, nullable
            )
            self.assertListEqual(update_column_nullability.field_name(), field_name)
            self.assertListEqual(update_column_nullability.get_field_name(), field_name)
            self.assertEqual(update_column_nullability.get_nullable(), nullable)

    def test_update_column_nullability_equal_and_hash(self):
        field_name = ["Column A"]
        nullable = True
        update_column_nullabilities = [
            TableChange.update_column_nullability(field_name, nullable)
            for _ in range(2)
        ]
        update_column_nullability_dict = {
            update_column_nullabilities[i]: i for i in range(2)
        }
        self.assertTrue(
            update_column_nullabilities[0] == update_column_nullabilities[1]
        )
        self.assertTrue(
            update_column_nullabilities[1] == update_column_nullabilities[0]
        )
        self.assertEqual(len(update_column_nullability_dict), 1)

        another_update_column_nullability = TableChange.update_column_nullability(
            ["Column B"], False
        )
        update_column_nullability_dict = {
            update_column_nullabilities[0]: 0,
            another_update_column_nullability: 1,
        }
        self.assertFalse(
            update_column_nullabilities[0] == another_update_column_nullability
        )
        self.assertFalse(
            another_update_column_nullability == update_column_nullabilities[0]
        )
        self.assertFalse(
            update_column_nullabilities[0] == "invalid_update_column_nullability"
        )
        self.assertEqual(len(update_column_nullability_dict), 2)

    def test_add_index(self):
        index_name = "index_name"
        field_names = [["id"]]
        index_type = Index.IndexType.PRIMARY_KEY
        add_index = TableChange.add_index(index_type, index_name, field_names)
        self.assertEqual(add_index.get_name(), index_name)
        self.assertListEqual(add_index.get_field_names(), field_names)
        self.assertIs(add_index.get_type(), index_type)

    def test_add_index_equal_and_hash(self):
        index_name = "index_name"
        field_names = [["Column A"], ["Column B"]]
        index_type = Index.IndexType.UNIQUE_KEY
        add_indexes = [
            TableChange.add_index(index_type, index_name, field_names) for _ in range(2)
        ]
        add_index_dict = {add_indexes[i]: i for i in range(2)}
        self.assertTrue(add_indexes[0] == add_indexes[1])
        self.assertTrue(add_indexes[1] == add_indexes[0])
        self.assertFalse(add_indexes[0] == "invalid_add_index")
        self.assertEqual(len(add_index_dict), 1)

        another_add_index = TableChange.add_index(
            Index.IndexType.PRIMARY_KEY, "another_index_name", [["id"]]
        )
        add_index_dict = {add_indexes[0]: 0, another_add_index: 1}
        self.assertFalse(add_indexes[0] == another_add_index)
        self.assertFalse(another_add_index == add_indexes[0])
        self.assertEqual(len(add_index_dict), 2)

    def test_delete_index(self):
        index_name = "index_name"
        if_exists = True
        delete_index = TableChange.delete_index(index_name, if_exists)
        self.assertEqual(delete_index.get_name(), index_name)
        self.assertEqual(delete_index.is_if_exists(), if_exists)

    def test_delete_index_equal_and_hash(self):
        index_name = "index_name"
        if_exists = True
        delete_indexes = [
            TableChange.delete_index(index_name, if_exists) for _ in range(2)
        ]
        delete_index_dict = {delete_indexes[i]: i for i in range(2)}
        self.assertTrue(delete_indexes[0] == delete_indexes[1])
        self.assertTrue(delete_indexes[1] == delete_indexes[0])
        self.assertFalse(delete_indexes[0] == "invalid_delete_index")
        self.assertEqual(len(delete_index_dict), 1)

        another_delete_index = TableChange.delete_index("another_index_name", if_exists)
        delete_index_dict = {delete_indexes[0]: 0, another_delete_index: 1}
        self.assertFalse(delete_indexes[0] == another_delete_index)
        self.assertFalse(another_delete_index == delete_indexes[0])
        self.assertEqual(len(delete_index_dict), 2)

    def test_update_column_auto_increment(self):
        field_name_data = [["existing_column"], ["nested", "existing_column"]]
        auto_increment_data = [True, False]
        for field_name, auto_increment in zip(field_name_data, auto_increment_data):
            update_column_auto_increment = TableChange.update_column_auto_increment(
                field_name, auto_increment
            )
            self.assertListEqual(update_column_auto_increment.field_name(), field_name)
            self.assertEqual(
                update_column_auto_increment.is_auto_increment(), auto_increment
            )

    def test_update_column_auto_increment_equal_and_hash(self):
        field_name = ["Column A"]
        auto_increment = True
        update_column_auto_increments = [
            TableChange.update_column_auto_increment(field_name, auto_increment)
            for _ in range(2)
        ]
        update_column_auto_increment_dict = {
            update_column_auto_increments[i]: i for i in range(2)
        }
        self.assertTrue(
            update_column_auto_increments[0] == update_column_auto_increments[1]
        )
        self.assertTrue(
            update_column_auto_increments[1] == update_column_auto_increments[0]
        )
        self.assertEqual(len(update_column_auto_increment_dict), 1)

        another_update_column_auto_increment = TableChange.update_column_auto_increment(
            ["Column B"], False
        )
        update_column_auto_increment_dict = {
            update_column_auto_increments[0]: 0,
            another_update_column_auto_increment: 1,
        }
        self.assertFalse(
            update_column_auto_increments[0] == another_update_column_auto_increment
        )
        self.assertFalse(
            another_update_column_auto_increment == update_column_auto_increments[0]
        )
        self.assertFalse(
            update_column_auto_increments[0] == "invalid_update_column_auto_increment"
        )
        self.assertEqual(len(update_column_auto_increment_dict), 2)
