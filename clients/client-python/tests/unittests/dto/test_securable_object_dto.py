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
from __future__ import annotations

import unittest

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO


class TestSecurableObjectDTO(unittest.TestCase):
    def test_create_securable_object_dto_simple_name(self):
        """Test with a simple fullName (no dots)."""
        privilege = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        dto = SecurableObjectDTO(
            _full_name="test_metalake",
            _type=MetadataObject.Type.METALAKE,
            _privileges=[privilege],
        )

        d = dto.to_dict()
        self.assertEqual(d["fullName"], "test_metalake")
        self.assertEqual(d["type"], MetadataObject.Type.METALAKE)
        self.assertEqual(len(d["privileges"]), 1)
        self.assertEqual(
            d["privileges"][0]["name"], "create_fileset"
        )
        self.assertEqual(
            d["privileges"][0]["condition"], "allow"
        )

    def test_create_securable_object_dto_dotted_name(self):
        """Test with a dotted fullName (e.g., catalog.schema)."""
        privilege = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )
        dto = SecurableObjectDTO(
            _full_name="catalog1.schema1",
            _type=MetadataObject.Type.SCHEMA,
            _privileges=[privilege],
        )

        # Verify fullName splitting in __post_init__
        self.assertEqual(dto.name(), "schema1")
        self.assertEqual(dto.parent(), "catalog1")
        self.assertEqual(dto.full_name(), "catalog1.schema1")

    def test_securable_object_dto_methods(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        dto = SecurableObjectDTO(
            _full_name="my_catalog",
            _type=MetadataObject.Type.CATALOG,
            _privileges=[privilege],
        )
        self.assertEqual(dto.name(), "my_catalog")
        self.assertIsNone(dto.parent())
        self.assertEqual(dto.full_name(), "my_catalog")
        self.assertEqual(dto.type(), MetadataObject.Type.CATALOG)
        self.assertEqual(len(dto.privileges()), 1)

    def test_builder(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.READ_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        dto = (
            SecurableObjectDTO.builder()
            .with_full_name("ml.catalog.schema")
            .with_type(MetadataObject.Type.SCHEMA)
            .with_privileges([privilege])
            .build()
        )
        self.assertEqual(dto.full_name(), "ml.catalog.schema")
        self.assertEqual(dto.name(), "schema")
        self.assertEqual(dto.parent(), "ml.catalog")

    def test_builder_no_full_name_raises(self):
        with self.assertRaises(ValueError):
            SecurableObjectDTO.builder().with_type(
                MetadataObject.Type.CATALOG
            ).build()

    def test_builder_no_type_raises(self):
        with self.assertRaises(ValueError):
            SecurableObjectDTO.builder().with_full_name("test").build()

    def test_equality_and_hash(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        dto1 = SecurableObjectDTO(
            _full_name="test_obj",
            _type=MetadataObject.Type.METALAKE,
            _privileges=[privilege],
        )
        dto2 = SecurableObjectDTO(
            _full_name="test_obj",
            _type=MetadataObject.Type.METALAKE,
            _privileges=[],  # different privileges, but equality is on fullName+type
        )
        dto3 = SecurableObjectDTO(
            _full_name="other_obj",
            _type=MetadataObject.Type.METALAKE,
            _privileges=[privilege],
        )

        self.assertEqual(dto1, dto2)
        self.assertEqual(hash(dto1), hash(dto2))
        self.assertNotEqual(dto1, dto3)
