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
from gravitino.api.rel.dialects import Dialects
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.types.types import Types
from gravitino.api.rel.view_change import (
    RemoveProperty,
    RenameView,
    ReplaceView,
    SetProperty,
    ViewChange,
)


class TestView(unittest.TestCase):
    def test_dialects(self):
        self.assertEqual("trino", Dialects.TRINO)
        self.assertEqual("spark", Dialects.SPARK)
        self.assertEqual("hive", Dialects.HIVE)
        self.assertEqual("flink", Dialects.FLINK)

    def test_sql_representation(self):
        representation = SQLRepresentation(Dialects.TRINO, "SELECT 1")

        self.assertEqual(Representation.TYPE_SQL, representation.type())
        self.assertEqual(Dialects.TRINO, representation.dialect())
        self.assertEqual("SELECT 1", representation.sql())
        self.assertEqual(representation, SQLRepresentation(Dialects.TRINO, "SELECT 1"))
        self.assertNotEqual(
            representation, SQLRepresentation(Dialects.SPARK, "SELECT 1")
        )

        with self.assertRaises(ValueError):
            SQLRepresentation("", "SELECT 1")
        with self.assertRaises(ValueError):
            SQLRepresentation(Dialects.TRINO, "")

    def test_view_change_rename(self):
        change = ViewChange.rename("new_view")

        self.assertIsInstance(change, RenameView)
        self.assertEqual("new_view", change.new_name())
        self.assertEqual(change, ViewChange.rename("new_view"))
        self.assertNotEqual(change, ViewChange.rename("another_view"))

        with self.assertRaises(ValueError):
            ViewChange.rename("")

    def test_view_change_set_property(self):
        change = ViewChange.set_property("key", "value")

        self.assertIsInstance(change, SetProperty)
        self.assertEqual("key", change.property())
        self.assertEqual("value", change.value())
        self.assertEqual(change, ViewChange.set_property("key", "value"))

        with self.assertRaises(ValueError):
            ViewChange.set_property("", "value")

    def test_view_change_remove_property(self):
        change = ViewChange.remove_property("key")

        self.assertIsInstance(change, RemoveProperty)
        self.assertEqual("key", change.property())
        self.assertEqual(change, ViewChange.remove_property("key"))

        with self.assertRaises(ValueError):
            ViewChange.remove_property("")

    def test_view_change_replace_view(self):
        columns = [Column.of("id", Types.IntegerType.get())]
        representations = [SQLRepresentation(Dialects.TRINO, "SELECT id FROM tbl")]

        change = ViewChange.replace_view(
            columns,
            representations,
            default_catalog="catalog",
            default_schema="schema",
            comment="comment",
        )

        self.assertIsInstance(change, ReplaceView)
        self.assertEqual(columns, change.columns())
        self.assertEqual(representations, change.representations())
        self.assertEqual("catalog", change.default_catalog())
        self.assertEqual("schema", change.default_schema())
        self.assertEqual("comment", change.comment())
        self.assertEqual(
            change,
            ViewChange.replace_view(
                columns, representations, "catalog", "schema", "comment"
            ),
        )

        with self.assertRaises(ValueError):
            ViewChange.replace_view(None, representations)
        with self.assertRaises(ValueError):
            ViewChange.replace_view(columns, [])
