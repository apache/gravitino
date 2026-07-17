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
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.types.types import Types
from gravitino.api.rel.view_change import (
    RemoveProperty,
    RenameView,
    ReplaceView,
    SetProperty,
    ViewChange,
)


class TestViewChange(unittest.TestCase):
    def test_rename_view(self):
        change = ViewChange.rename("new_view")
        equal_change = ViewChange.rename("new_view")

        self.assertIsInstance(change, RenameView)
        self.assertEqual("new_view", change.new_name())
        self.assertEqual("RENAMEVIEW new_view", str(change))
        self.assertEqual(change, equal_change)
        self.assertEqual(hash(change), hash(equal_change))
        self.assertNotEqual(change, ViewChange.rename("another_view"))
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(ValueError, "newName must not be null or empty"):
            ViewChange.rename("")

    def test_set_property(self):
        change = ViewChange.set_property("key", "value")
        equal_change = ViewChange.set_property("key", "value")

        self.assertIsInstance(change, SetProperty)
        self.assertEqual("key", change.property())
        self.assertEqual("value", change.value())
        self.assertEqual("SETPROPERTY key value", str(change))
        self.assertEqual(change, equal_change)
        self.assertEqual(hash(change), hash(equal_change))
        self.assertNotEqual(change, ViewChange.set_property("key", "another_value"))
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(ValueError, "property must not be null or empty"):
            ViewChange.set_property("", "value")

    def test_remove_property(self):
        change = ViewChange.remove_property("key")
        equal_change = ViewChange.remove_property("key")

        self.assertIsInstance(change, RemoveProperty)
        self.assertEqual("key", change.property())
        self.assertEqual("REMOVEPROPERTY key", str(change))
        self.assertEqual(change, equal_change)
        self.assertEqual(hash(change), hash(equal_change))
        self.assertNotEqual(change, ViewChange.remove_property("another_key"))
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(ValueError, "property must not be null or empty"):
            ViewChange.remove_property("")

    def test_replace_view(self):
        columns = [Column.of("id", Types.IntegerType.get())]
        representations = [SQLRepresentation(Dialects.TRINO, "SELECT id FROM tbl")]
        change = ViewChange.replace_view(
            columns,
            representations,
            default_catalog="catalog",
            default_schema="schema",
            comment="comment",
        )
        equal_change = ViewChange.replace_view(
            [Column.of("id", Types.IntegerType.get())],
            [SQLRepresentation(Dialects.TRINO, "SELECT id FROM tbl")],
            "catalog",
            "schema",
            "comment",
        )

        self.assertIsInstance(change, ReplaceView)
        self.assertEqual(columns, change.columns())
        self.assertEqual(representations, change.representations())
        self.assertEqual("catalog", change.default_catalog())
        self.assertEqual("schema", change.default_schema())
        self.assertEqual("comment", change.comment())
        self.assertEqual(
            f"REPLACEVIEW columns={columns}, representations={representations}, "
            "defaultCatalog=catalog, defaultSchema=schema, comment=comment",
            str(change),
        )
        self.assertEqual(change, equal_change)
        self.assertEqual(hash(change), hash(equal_change))
        self.assertNotEqual(
            change,
            ViewChange.replace_view(
                columns, representations, "another_catalog", "schema", "comment"
            ),
        )
        self.assertNotEqual(change, "invalid_change")

    def test_replace_view_defensively_copies_lists(self):
        columns = [Column.of("id", Types.IntegerType.get())]
        representations = [SQLRepresentation(Dialects.TRINO, "SELECT id FROM tbl")]
        change = ViewChange.replace_view(columns, representations)
        original_hash = hash(change)

        columns[0] = Column.of("name", Types.StringType.get())
        representations[0] = SQLRepresentation(Dialects.SPARK, "SELECT name FROM tbl")

        self.assertEqual("id", change.columns()[0].name())
        self.assertEqual(Dialects.TRINO, change.representations()[0].dialect())

        returned_columns = change.columns()
        returned_representations = change.representations()
        returned_columns[0] = Column.of("age", Types.IntegerType.get())
        returned_representations[0] = SQLRepresentation(
            Dialects.HIVE, "SELECT age FROM tbl"
        )

        self.assertEqual("id", change.columns()[0].name())
        self.assertEqual(Dialects.TRINO, change.representations()[0].dialect())
        self.assertEqual(original_hash, hash(change))

    def test_replace_view_validation(self):
        representations = [SQLRepresentation(Dialects.TRINO, "SELECT 1")]

        with self.assertRaisesRegex(ValueError, "columns must not be null"):
            ViewChange.replace_view(None, representations)
        with self.assertRaisesRegex(
            ValueError, "representations must not be null or empty"
        ):
            ViewChange.replace_view([], [])
