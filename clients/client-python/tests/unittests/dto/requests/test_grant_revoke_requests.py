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
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.requests.privilege_grant_request import PrivilegeGrantRequest
from gravitino.dto.requests.privilege_revoke_request import PrivilegeRevokeRequest
from gravitino.dto.requests.role_grant_request import RoleGrantRequest
from gravitino.dto.requests.role_revoke_request import RoleRevokeRequest


class TestRoleGrantRequest(unittest.TestCase):
    def test_create(self):
        req = RoleGrantRequest(["role1", "role2"])
        data = json.loads(req.to_json())
        self.assertEqual(["role1", "role2"], data["roleNames"])

    def test_validate(self):
        req = RoleGrantRequest(["role1"])
        req.validate()

    def test_validate_empty_rejected(self):
        req = RoleGrantRequest([])
        with self.assertRaises(ValueError):
            req.validate()

    def test_json_roundtrip(self):
        req = RoleGrantRequest(["role1"])
        encoded = req.to_json()
        self.assertIn("roleNames", encoded)


class TestRoleRevokeRequest(unittest.TestCase):
    def test_create(self):
        req = RoleRevokeRequest(["role1"])
        data = json.loads(req.to_json())
        self.assertEqual(["role1"], data["roleNames"])

    def test_validate_empty_rejected(self):
        req = RoleRevokeRequest([])
        with self.assertRaises(ValueError):
            req.validate()


class TestPrivilegeGrantRequest(unittest.TestCase):
    def test_create(self):
        privileges = [
            PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)
        ]
        req = PrivilegeGrantRequest(privileges)
        data = json.loads(req.to_json())
        self.assertEqual(1, len(data["privileges"]))

    def test_validate_empty_rejected(self):
        req = PrivilegeGrantRequest([])
        with self.assertRaises(ValueError):
            req.validate()

    def test_json_roundtrip(self):
        privileges = [
            PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)
        ]
        req = PrivilegeGrantRequest(privileges)
        encoded = req.to_json()
        self.assertIn("privileges", encoded)


class TestPrivilegeRevokeRequest(unittest.TestCase):
    def test_create(self):
        privileges = [
            PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.DENY)
        ]
        req = PrivilegeRevokeRequest(privileges)
        data = json.loads(req.to_json())
        self.assertEqual(1, len(data["privileges"]))

    def test_validate_empty_rejected(self):
        req = PrivilegeRevokeRequest([])
        with self.assertRaises(ValueError):
            req.validate()
