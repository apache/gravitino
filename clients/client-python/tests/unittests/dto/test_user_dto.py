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

from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.user_dto import UserDTO


class TestUserDTO(unittest.TestCase):
    def test_create_user_dto(self):
        audit = AuditDTO(
            _creator="admin",
            _create_time="2024-01-01T00:00:00Z",
        )
        user_dto = (
            UserDTO.builder()
            .with_name("test_user")
            .with_roles(["role1", "role2"])
            .with_audit(audit)
            .build()
        )

        ser_json = _json.dumps(user_dto.to_dict()).encode("utf-8")
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "test_user")
        self.assertEqual(deser_dict["roles"], ["role1", "role2"])
        self.assertEqual(deser_dict["audit"]["creator"], "admin")

    def test_user_dto_without_roles(self):
        user_dto = UserDTO.builder().with_name("test_user_no_roles").build()

        ser_json = _json.dumps(user_dto.to_dict()).encode("utf-8")
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "test_user_no_roles")
        self.assertEqual(deser_dict["roles"], [])

    def test_user_dto_methods(self):
        user_dto = (
            UserDTO.builder()
            .with_name("method_user")
            .with_roles(["admin_role"])
            .build()
        )
        self.assertEqual(user_dto.name(), "method_user")
        self.assertEqual(user_dto.roles(), ["admin_role"])
        self.assertIsNone(user_dto.audit_info())

    def test_builder_empty_name_raises(self):
        with self.assertRaises(ValueError):
            UserDTO.builder().build()

    def test_equality_and_hash(self):
        audit = AuditDTO(_creator="test", _create_time="2024-01-01T00:00:00Z")

        user1 = (
            UserDTO.builder()
            .with_name("user1")
            .with_roles(["r1"])
            .with_audit(audit)
            .build()
        )
        user2 = (
            UserDTO.builder()
            .with_name("user1")
            .with_roles(["r1"])
            .with_audit(audit)
            .build()
        )
        user3 = (
            UserDTO.builder()
            .with_name("user2")
            .with_roles(["r1"])
            .with_audit(audit)
            .build()
        )

        self.assertEqual(user1, user2)
        self.assertEqual(hash(user1), hash(user2))
        self.assertNotEqual(user1, user3)
