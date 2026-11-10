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

from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.api.authorization.privileges import Privilege
from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO


class TestRoleDTO(unittest.TestCase):
    def _audit(self) -> AuditDTO:
        return AuditDTO(_creator="admin", _create_time="2024-01-01T00:00:00Z")

    def test_create_role_dto(self):
        sec_objs = [
            SecurableObjectDTO(
                "my_catalog",
                MetadataObject.Type.CATALOG,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
        ]
        role = (
            RoleDTO.builder()
            .with_name("admin_role")
            .with_properties({"k": "v"})
            .with_securable_objects(sec_objs)
            .with_audit(self._audit())
            .build()
        )
        self.assertEqual("admin_role", role.name())
        self.assertEqual({"k": "v"}, role.properties())
        self.assertEqual(1, len(role.securable_objects()))

    def test_role_dto_without_properties_and_objects(self):
        role = (
            RoleDTO.builder().with_name("empty_role").with_audit(self._audit()).build()
        )
        self.assertIsNone(role.properties())
        self.assertEqual([], role.securable_objects())

    def test_role_dto_methods(self):
        role = (
            RoleDTO.builder()
            .with_name("test_role")
            .with_properties({"a": "b"})
            .with_audit(self._audit())
            .build()
        )
        self.assertEqual("test_role", role.name())
        self.assertEqual({"a": "b"}, role.properties())
        self.assertEqual([], role.securable_objects())
        self.assertIsNotNone(role.audit_info())

    def test_builder_empty_name_raises(self):
        with self.assertRaises(ValueError):
            RoleDTO.builder().with_audit(self._audit()).build()

    def test_builder_no_audit_raises(self):
        with self.assertRaises(ValueError):
            RoleDTO.builder().with_name("test").build()

    def test_equality_and_hash(self):
        role1 = (
            RoleDTO.builder()
            .with_name("role1")
            .with_properties({"k": "v"})
            .with_audit(self._audit())
            .build()
        )
        role2 = (
            RoleDTO.builder()
            .with_name("role1")
            .with_properties({"k": "v"})
            .with_audit(self._audit())
            .build()
        )
        role3 = (
            RoleDTO.builder()
            .with_name("role2")
            .with_properties({"k": "v"})
            .with_audit(self._audit())
            .build()
        )
        self.assertEqual(role1, role2)
        self.assertEqual(hash(role1), hash(role2))
        self.assertNotEqual(role1, role3)

    def test_json_roundtrip(self):
        sec_objs = [
            SecurableObjectDTO(
                "catalog",
                MetadataObject.Type.CATALOG,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
        ]
        role = (
            RoleDTO.builder()
            .with_name("json_role")
            .with_properties({"k": "v"})
            .with_securable_objects(sec_objs)
            .with_audit(self._audit())
            .build()
        )
        encoded = _json.dumps(role.to_dict()).encode("utf-8")
        decoded = _json.loads(encoded)
        self.assertEqual("json_role", decoded["name"])
        self.assertEqual({"k": "v"}, decoded["properties"])
        self.assertEqual(1, len(decoded["securableObjects"]))
