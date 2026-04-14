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
from gravitino.dto.authorization.group_dto import GroupDTO


class TestGroupDTO(unittest.TestCase):
    def test_create_group_dto(self):
        audit = AuditDTO(
            _creator="admin",
            _create_time="2024-01-01T00:00:00Z",
        )
        group_dto = (
            GroupDTO.builder()
            .with_name("test_group")
            .with_roles(["role1", "role2"])
            .with_audit(audit)
            .build()
        )

        ser_json = _json.dumps(group_dto.to_dict()).encode("utf-8")
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "test_group")
        self.assertEqual(deser_dict["roles"], ["role1", "role2"])
        self.assertEqual(deser_dict["audit"]["creator"], "admin")

    def test_group_dto_without_roles(self):
        group_dto = GroupDTO.builder().with_name("test_group_no_roles").build()

        ser_json = _json.dumps(group_dto.to_dict()).encode("utf-8")
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "test_group_no_roles")
        self.assertEqual(deser_dict["roles"], [])

    def test_group_dto_methods(self):
        group_dto = (
            GroupDTO.builder()
            .with_name("method_group")
            .with_roles(["admin_role"])
            .build()
        )
        self.assertEqual(group_dto.name(), "method_group")
        self.assertEqual(group_dto.roles(), ["admin_role"])
        self.assertIsNone(group_dto.audit_info())

    def test_builder_empty_name_raises(self):
        with self.assertRaises(ValueError):
            GroupDTO.builder().build()

    def test_equality_and_hash(self):
        audit = AuditDTO(_creator="test", _create_time="2024-01-01T00:00:00Z")

        group1 = (
            GroupDTO.builder()
            .with_name("group1")
            .with_roles(["r1"])
            .with_audit(audit)
            .build()
        )
        group2 = (
            GroupDTO.builder()
            .with_name("group1")
            .with_roles(["r1"])
            .with_audit(audit)
            .build()
        )
        group3 = (
            GroupDTO.builder()
            .with_name("group2")
            .with_roles(["r1"])
            .with_audit(audit)
            .build()
        )

        self.assertEqual(group1, group2)
        self.assertEqual(hash(group1), hash(group2))
        self.assertNotEqual(group1, group3)
