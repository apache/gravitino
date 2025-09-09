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

from gravitino.api.table_change import TableChange


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
