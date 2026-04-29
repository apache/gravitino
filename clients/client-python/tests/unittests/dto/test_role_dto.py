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

import json as _json
import unittest

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO


class TestRoleDTO(unittest.TestCase):
    def test_create_role_dto(self):
        audit = AuditDTO(
            _creator="admin",
            _create_time="2024-01-01T00:00:00Z",
        )
        privilege = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        securable_obj = SecurableObjectDTO(
            _full_name="test_metalake",
            _type=MetadataObject.Type.METALAKE,
            _privileges=[privilege],
        )

        role_dto = (
            RoleDTO.builder()
            .with_name("test_role")
            .with_properties({"purpose": "test"})
            .with_securable_objects([securable_obj])
            .with_audit(audit)
            .build()
        )

        d = role_dto.to_dict()
        self.assertEqual(d["name"], "test_role")
        self.assertEqual(d["properties"], {"purpose": "test"})
        self.assertEqual(len(d["securableObjects"]), 1)
        self.assertEqual(
            d["securableObjects"][0]["fullName"], "test_metalake"
        )
        self.assertEqual(d["audit"]["creator"], "admin")

    def test_role_dto_without_properties_and_objects(self):
        role_dto = RoleDTO.builder().with_name("minimal_role").build()

        ser_json = _json.dumps(role_dto.to_dict()).encode("utf-8")
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "minimal_role")
        self.assertIsNone(deser_dict["properties"])
        self.assertEqual(deser_dict["securableObjects"], [])

    def test_role_dto_methods(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )
        securable_obj = SecurableObjectDTO(
            _full_name="catalog1",
            _type=MetadataObject.Type.CATALOG,
            _privileges=[privilege],
        )
        role_dto = (
            RoleDTO.builder()
            .with_name("method_role")
            .with_properties({"k": "v"})
            .with_securable_objects([securable_obj])
            .build()
        )
        self.assertEqual(role_dto.name(), "method_role")
        self.assertEqual(role_dto.properties(), {"k": "v"})
        self.assertEqual(len(role_dto.securable_objects()), 1)
        self.assertIsNone(role_dto.audit_info())

    def test_builder_empty_name_raises(self):
        with self.assertRaises(ValueError):
            RoleDTO.builder().build()

    def test_equality_and_hash(self):
        audit = AuditDTO(_creator="test", _create_time="2024-01-01T00:00:00Z")

        role1 = (
            RoleDTO.builder()
            .with_name("role1")
            .with_properties({"k": "v"})
            .with_audit(audit)
            .build()
        )
        role2 = (
            RoleDTO.builder()
            .with_name("role1")
            .with_properties({"k": "v"})
            .with_audit(audit)
            .build()
        )
        role3 = (
            RoleDTO.builder()
            .with_name("role2")
            .with_properties({"k": "v"})
            .with_audit(audit)
            .build()
        )

        self.assertEqual(role1, role2)
        self.assertEqual(hash(role1), hash(role2))
        self.assertNotEqual(role1, role3)
