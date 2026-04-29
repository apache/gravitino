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

from gravitino.api.authorization.privileges import Privilege, Privileges
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO


class TestPrivilegeDTO(unittest.TestCase):
    def test_create_allow_privilege_dto(self):
        dto = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )

        d = dto.to_dict()
        self.assertEqual(d["name"], "create_fileset")
        self.assertEqual(d["condition"], "allow")

    def test_create_deny_privilege_dto(self):
        dto = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.DENY,
        )

        d = dto.to_dict()
        self.assertEqual(d["name"], "use_catalog")
        self.assertEqual(d["condition"], "deny")

    def test_privilege_dto_methods(self):
        dto = PrivilegeDTO(
            _name=Privilege.Name.CREATE_TABLE,
            _condition=Privilege.Condition.ALLOW,
        )
        self.assertEqual(dto.name(), Privilege.Name.CREATE_TABLE)
        self.assertEqual(dto.condition(), Privilege.Condition.ALLOW)
        self.assertEqual(dto.simple_string(), "ALLOW create table")
        self.assertTrue(dto.can_bind_to(None))

    def test_privilege_dto_json_roundtrip(self):
        dto = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        ser_json = _json.dumps(dto.to_dict())
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "create_fileset")
        self.assertEqual(deser_dict["condition"], "allow")

        # Verify roundtrip deserialization
        restored = PrivilegeDTO.from_dict(deser_dict)
        self.assertEqual(restored.name(), Privilege.Name.CREATE_FILESET)
        self.assertEqual(restored.condition(), Privilege.Condition.ALLOW)

    def test_builder(self):
        dto = (
            PrivilegeDTO.builder()
            .with_name(Privilege.Name.READ_FILESET)
            .with_condition(Privilege.Condition.ALLOW)
            .build()
        )
        self.assertEqual(dto.name(), Privilege.Name.READ_FILESET)
        self.assertEqual(dto.condition(), Privilege.Condition.ALLOW)

    def test_builder_no_name_raises(self):
        with self.assertRaises(ValueError):
            PrivilegeDTO.builder().with_condition(Privilege.Condition.ALLOW).build()

    def test_builder_no_condition_raises(self):
        with self.assertRaises(ValueError):
            PrivilegeDTO.builder().with_name(Privilege.Name.CREATE_FILESET).build()

    def test_equality_and_hash(self):
        dto1 = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        dto2 = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        dto3 = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.DENY,
        )
        dto4 = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )

        self.assertEqual(dto1, dto2)
        self.assertEqual(hash(dto1), hash(dto2))
        self.assertNotEqual(dto1, dto3)
        self.assertNotEqual(dto1, dto4)

    def test_equality_with_generic_privilege(self):
        """PrivilegeDTO and _GenericPrivilege should be symmetrically equal."""
        dto = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        generic = Privileges.allow(Privilege.Name.CREATE_FILESET)
        # Bidirectional equality
        self.assertEqual(dto, generic)
        self.assertEqual(generic, dto)
