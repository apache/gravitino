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

from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.responses.role_response import (
    RoleNamesListResponse,
    RoleResponse,
)


class TestRoleResponse(unittest.TestCase):
    def test_role_response(self):
        role = (
            RoleDTO.builder()
            .with_name("test_role")
            .with_audit(AuditDTO(_creator="admin", _create_time="2024-01-01T00:00:00Z"))
            .build()
        )
        resp = RoleResponse(0, role)
        resp.validate()
        self.assertEqual(role, resp.role())

    def test_role_names_list_response(self):
        resp = RoleNamesListResponse(0, ["role1", "role2"])
        resp.validate()
        self.assertEqual(["role1", "role2"], resp.names())

    def test_validate_no_role(self):
        resp = RoleResponse(0, None)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_validate_no_names(self):
        resp = RoleNamesListResponse(0, None)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_json_roundtrip(self):
        role = (
            RoleDTO.builder()
            .with_name("json_role")
            .with_audit(AuditDTO(_creator="admin", _create_time="2024-01-01T00:00:00Z"))
            .build()
        )
        resp = RoleResponse(0, role)
        json_str = resp.to_json()
        restored = RoleResponse.from_json(json_str, infer_missing=True)
        restored.validate()
        self.assertEqual("json_role", restored.role().name())
