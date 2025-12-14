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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.exceptions.base import IllegalArgumentException


class TestMetadataObjects(unittest.TestCase):
    def test_of_with_string_name(self):
        obj = MetadataObjects.of("catalog1", MetadataObject.Type.CATALOG)
        self.assertEqual("catalog1", obj.name())
        self.assertEqual(MetadataObject.Type.CATALOG, obj.type())
        self.assertIsNone(obj.parent())

    def test_of_with_string_name_and_parent(self):
        obj = MetadataObjects.of("schema1", MetadataObject.Type.SCHEMA, "catalog1")
        self.assertEqual("schema1", obj.name())
        self.assertEqual(MetadataObject.Type.SCHEMA, obj.type())
        self.assertEqual("catalog1", obj.parent())

    def test_of_with_list_names_metalake(self):
        obj = MetadataObjects.of(["metalake1"], MetadataObject.Type.METALAKE)
        self.assertEqual("metalake1", obj.name())
        self.assertEqual(MetadataObject.Type.METALAKE, obj.type())
        self.assertIsNone(obj.parent())

    def test_of_with_list_names_catalog(self):
        obj = MetadataObjects.of(["catalog1"], MetadataObject.Type.CATALOG)
        self.assertEqual("catalog1", obj.name())
        self.assertEqual(MetadataObject.Type.CATALOG, obj.type())
        self.assertIsNone(obj.parent())

    def test_of_with_list_names_role(self):
        obj = MetadataObjects.of(["role1"], MetadataObject.Type.ROLE)
        self.assertEqual("role1", obj.name())
        self.assertEqual(MetadataObject.Type.ROLE, obj.type())
        self.assertIsNone(obj.parent())

    def test_of_with_list_names_schema(self):
        obj = MetadataObjects.of(["catalog1", "schema1"], MetadataObject.Type.SCHEMA)
        self.assertEqual("schema1", obj.name())
        self.assertEqual(MetadataObject.Type.SCHEMA, obj.type())
        self.assertEqual("catalog1", obj.parent())

    def test_of_with_list_names_table(self):
        obj = MetadataObjects.of(
            ["catalog1", "schema1", "table1"], MetadataObject.Type.TABLE
        )
        self.assertEqual("table1", obj.name())
        self.assertEqual(MetadataObject.Type.TABLE, obj.type())
        self.assertEqual("catalog1.schema1", obj.parent())

    def test_of_with_list_names_column(self):
        obj = MetadataObjects.of(
            ["catalog1", "schema1", "table1", "col1"], MetadataObject.Type.COLUMN
        )
        self.assertEqual("col1", obj.name())
        self.assertEqual(MetadataObject.Type.COLUMN, obj.type())
        self.assertEqual("catalog1.schema1.table1", obj.parent())

    def test_of_invalid_names(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Cannot create a metadata object with no names"
        ):
            MetadataObjects.of([], MetadataObject.Type.CATALOG)
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "Cannot create a metadata object with the name length which is greater than 4",
        ):
            MetadataObjects.of(["a", "b", "c", "d", "e"], MetadataObject.Type.CATALOG)

    def test_of_invalid_name_length_type_mismatch(self):
        with self.assertRaisesRegex(
            IllegalArgumentException,
            "If the length of names is 1, it must be the CATALOG, METALAKE, or ROLE type",
        ):
            MetadataObjects.of(["catalog1"], MetadataObject.Type.SCHEMA)

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "If the length of names is 2, it must be the SCHEMA type",
        ):
            MetadataObjects.of(["catalog1", "schema1"], MetadataObject.Type.CATALOG)

        with self.assertRaisesRegex(
            IllegalArgumentException,
            "If the length of names is 3, it must be FILESET, TABLE, TOPIC or MODEL",
        ):
            MetadataObjects.of(
                ["catalog1", "schema1", "table1"], MetadataObject.Type.COLUMN
            )

        with self.assertRaisesRegex(
            IllegalArgumentException, "If the length of names is 4, it must be COLUMN"
        ):
            MetadataObjects.of(
                ["catalog1", "schema1", "table1", "column1"], MetadataObject.Type.TABLE
            )

    def test_of_invalid_reserved_name(self):
        with self.assertRaises(IllegalArgumentException):
            MetadataObjects.of(["*"], MetadataObject.Type.CATALOG)

    def test_parent_root_object(self):
        obj = MetadataObjects.of(["metalake1"], MetadataObject.Type.METALAKE)
        self.assertIsNone(MetadataObjects.parent(obj))

        obj = MetadataObjects.of(["catalog1"], MetadataObject.Type.CATALOG)
        self.assertIsNone(MetadataObjects.parent(obj))

        obj = MetadataObjects.of(["role1"], MetadataObject.Type.ROLE)
        self.assertIsNone(MetadataObjects.parent(obj))

    def test_parent_schema(self):
        obj = MetadataObjects.of(["catalog1", "schema1"], MetadataObject.Type.SCHEMA)
        parent = MetadataObjects.parent(obj)
        self.assertIsNotNone(parent)
        self.assertEqual("catalog1", parent.name())
        self.assertEqual(MetadataObject.Type.CATALOG, parent.type())

    def test_parent_table(self):
        obj = MetadataObjects.of(
            ["catalog1", "schema1", "table1"], MetadataObject.Type.TABLE
        )
        parent = MetadataObjects.parent(obj)
        self.assertIsNotNone(parent)
        self.assertEqual("schema1", parent.name())
        self.assertEqual(MetadataObject.Type.SCHEMA, parent.type())
        self.assertEqual("catalog1", parent.parent())

    def test_parent_column(self):
        obj = MetadataObjects.of(
            ["catalog1", "schema1", "table1", "col1"], MetadataObject.Type.COLUMN
        )
        parent = MetadataObjects.parent(obj)
        self.assertIsNotNone(parent)
        self.assertEqual("table1", parent.name())
        self.assertEqual(MetadataObject.Type.TABLE, parent.type())
        self.assertEqual("catalog1.schema1", parent.parent())

    def test_parent_none_input(self):
        self.assertIsNone(MetadataObjects.parent(None))

    def test_get_parent_full_name(self):
        self.assertIsNone(MetadataObjects.get_parent_full_name(["single"]))
        self.assertEqual(
            "catalog1", MetadataObjects.get_parent_full_name(["catalog1", "schema1"])
        )
        self.assertEqual(
            "catalog1.schema1",
            MetadataObjects.get_parent_full_name(["catalog1", "schema1", "table1"]),
        )
        self.assertEqual(
            "catalog1.schema1.table1",
            MetadataObjects.get_parent_full_name(
                ["catalog1", "schema1", "table1", "col1"]
            ),
        )

    def test_get_last_name(self):
        self.assertEqual("single", MetadataObjects.get_last_name(["single"]))
        self.assertEqual(
            "schema1", MetadataObjects.get_last_name(["catalog1", "schema1"])
        )
        self.assertEqual(
            "table1", MetadataObjects.get_last_name(["catalog1", "schema1", "table1"])
        )

    # def test_check_name_valid(self):
    #     MetadataObjects.check_name("valid_name")
    #     MetadataObjects.check_name("catalog1")

    # def test_check_name_invalid_none(self):
    #     with self.assertRaises(IllegalArgumentException):
    #         MetadataObjects.check_name(None)

    # def test_check_name_invalid_reserved(self):
    #     with self.assertRaises(IllegalArgumentException):
    #         MetadataObjects.check_name("*")

    def test_parse_root_object(self):
        obj = MetadataObjects.parse("metalake1", MetadataObject.Type.METALAKE)
        self.assertEqual("metalake1", obj.name())
        self.assertEqual(MetadataObject.Type.METALAKE, obj.type())
        self.assertIsNone(obj.parent())

        obj = MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG)
        self.assertEqual("catalog1", obj.name())
        self.assertEqual(MetadataObject.Type.CATALOG, obj.type())
        self.assertIsNone(obj.parent())

        obj = MetadataObjects.parse("role.with.dots", MetadataObject.Type.ROLE)
        self.assertEqual("role.with.dots", obj.name())
        self.assertEqual(MetadataObject.Type.ROLE, obj.type())
        self.assertIsNone(obj.parent())

    def test_parse_schema(self):
        obj = MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA)
        self.assertEqual("schema1", obj.name())
        self.assertEqual(MetadataObject.Type.SCHEMA, obj.type())
        self.assertEqual("catalog1", obj.parent())

    def test_parse_table(self):
        obj = MetadataObjects.parse(
            "catalog1.schema1.table1", MetadataObject.Type.TABLE
        )
        self.assertEqual("table1", obj.name())
        self.assertEqual(MetadataObject.Type.TABLE, obj.type())
        self.assertEqual("catalog1.schema1", obj.parent())

    def test_parse_invalid_blank_name(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Metadata object full name cannot be blank"
        ):
            MetadataObjects.parse("", MetadataObject.Type.CATALOG)

        with self.assertRaisesRegex(
            IllegalArgumentException, "Metadata object full name cannot be blank"
        ):
            MetadataObjects.parse("   ", MetadataObject.Type.CATALOG)

        with self.assertRaisesRegex(
            IllegalArgumentException, "Metadata object full name cannot be blank"
        ):
            MetadataObjects.parse(None, MetadataObject.Type.CATALOG)

    def test_metadata_object_impl_equal_and_hash(self):
        obj1 = MetadataObjects.of(["catalog1"], MetadataObject.Type.CATALOG)
        obj2 = MetadataObjects.of(["catalog1"], MetadataObject.Type.CATALOG)
        obj3 = MetadataObjects.of(["catalog2"], MetadataObject.Type.CATALOG)

        self.assertEqual(obj1, obj2)
        self.assertNotEqual(obj1, obj3)
        self.assertNotEqual(obj1, "not_a_metadata_object")
        self.assertEqual(hash(obj1), hash(obj2))
        self.assertNotEqual(hash(obj1), hash(obj3))

    def test_metadata_object_impl_str(self):
        obj = MetadataObjects.of(["catalog1", "schema1"], MetadataObject.Type.SCHEMA)
        expected_str = (
            f"MetadataObject: [fullName={obj.full_name()}], [type={obj.type().value}]"
        )
        self.assertEqual(expected_str, str(obj))
