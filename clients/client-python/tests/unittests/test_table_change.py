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

from gravitino.api.column import Column
from gravitino.api.table_change import TableChange
from gravitino.api.types.types import Types


class TestTableChange(unittest.TestCase):
    def test_table_change_rename(self):
        rename1, rename2 = (
            TableChange.rename(f"New table name {i + 1}") for i in range(2)
        )
        self.assertEqual(rename1.get_new_name(), "New table name 1")
        self.assertEqual(str(rename1), f"RENAMETABLE {rename1.get_new_name()}")
        self.assertFalse(rename1 == rename2)
        self.assertFalse(rename1 == "invalid_rename")
        self.assertTrue(rename1 == TableChange.rename("New table name 1"))

    def test_table_change_update_comment(self):
        new_comment1, new_comment2 = (
            TableChange.update_comment(f"New comment {i + 1}") for i in range(2)
        )
        self.assertEqual(new_comment1.get_new_comment(), "New comment 1")
        self.assertEqual(
            str(new_comment1), f"UPDATECOMMENT {new_comment1.get_new_comment()}"
        )
        self.assertFalse(new_comment1 == new_comment2)
        self.assertFalse(new_comment1 == "invalid_update_comment")
        self.assertTrue(new_comment1 == TableChange.update_comment("New comment 1"))

    def test_table_change_set_property(self):
        new_property1, new_property2 = (
            TableChange.set_property(f"new_property_{i + 1}", str(i + 1))
            for i in range(2)
        )
        self.assertEqual(new_property1.get_property(), "new_property_1")
        self.assertEqual(new_property1.get_value(), "1")
        self.assertEqual(
            str(new_property1),
            f"SETPROPERTY {new_property1.get_property()} {new_property1.get_value()}",
        )
        self.assertFalse(new_property1 == new_property2)
        self.assertFalse(new_property1 == "invalid_set_property")
        self.assertTrue(
            new_property1 == TableChange.set_property("new_property_1", "1")
        )

    def test_table_change_remove_property(self):
        property1, property2 = (
            TableChange.remove_property(f"property_{i + 1}") for i in range(2)
        )
        self.assertEqual(property1.get_property(), "property_1")
        self.assertEqual(str(property1), f"REMOVEPROPERTY {property1.get_property()}")
        self.assertFalse(property1 == property2)
        self.assertFalse(property1 == "invalid_remove_property")
        self.assertTrue(property1 == TableChange.remove_property("property_1"))

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
        self.assertEqual(len(add_column_dict), 1)
        self.assertEqual(add_column_dict[add_columns[0]], 1)

        another_add_column = TableChange.add_column(
            another_field_name, data_type, comment
        )
        add_column_dict = {add_columns[0]: 0, another_add_column: 1}
        self.assertFalse(add_columns[0] == another_add_column)
        self.assertFalse(another_add_column == add_columns[0])
        self.assertEqual(len(add_column_dict), 2)
