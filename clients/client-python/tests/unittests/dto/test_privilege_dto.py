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

import json
import unittest

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.privileges import Privileges
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO


class TestPrivilegeDTO(unittest.TestCase):
    def test_create_privilege_dto(self):
        dto = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        self.assertEqual(Privilege.Name.CREATE_FILESET, dto.name())
        self.assertEqual(Privilege.Condition.ALLOW, dto.condition())

    def test_simple_string(self):
        dto = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        self.assertEqual("ALLOW create fileset", dto.simple_string())

    def test_can_bind_to(self):
        dto = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        self.assertTrue(dto.can_bind_to(None))

    def test_equality_and_hash(self):
        dto1 = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        dto2 = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        dto3 = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.DENY)
        self.assertEqual(dto1, dto2)
        self.assertEqual(hash(dto1), hash(dto2))
        self.assertNotEqual(dto1, dto3)

    def test_equality_with_generic_privilege(self):
        dto = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        generic = Privileges.allow("CREATE_FILESET")
        self.assertEqual(dto, generic)
        self.assertEqual(generic, dto)

    def test_builder_validations(self):
        with self.assertRaises(ValueError):
            PrivilegeDTO.builder().with_condition(Privilege.Condition.ALLOW).build()

        with self.assertRaises(ValueError):
            PrivilegeDTO.builder().with_name(Privilege.Name.CREATE_FILESET).build()

    def test_json_roundtrip(self):
        dto = PrivilegeDTO(Privilege.Name.CREATE_FILESET, Privilege.Condition.ALLOW)
        encoded = dto.to_json()
        decoded = json.loads(encoded)
        self.assertEqual("create_fileset", decoded["name"])
        self.assertEqual("allow", decoded["condition"])

        restored = PrivilegeDTO.from_json(encoded)
        self.assertEqual(dto, restored)

    def test_builder(self):
        dto = (
            PrivilegeDTO.builder()
            .with_name(Privilege.Name.USE_CATALOG)
            .with_condition(Privilege.Condition.DENY)
            .build()
        )
        self.assertEqual(Privilege.Name.USE_CATALOG, dto.name())
        self.assertEqual(Privilege.Condition.DENY, dto.condition())
