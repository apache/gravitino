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
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO
from gravitino.dto.requests.group_add_request import GroupAddRequest
from gravitino.dto.requests.privilege_grant_request import PrivilegeGrantRequest
from gravitino.dto.requests.privilege_revoke_request import PrivilegeRevokeRequest
from gravitino.dto.requests.role_create_request import RoleCreateRequest
from gravitino.dto.requests.role_grant_request import RoleGrantRequest
from gravitino.dto.requests.role_revoke_request import RoleRevokeRequest
from gravitino.dto.requests.user_add_request import UserAddRequest


class TestUserAddRequest(unittest.TestCase):
    def test_user_add_request_serde(self):
        req = UserAddRequest("test_user")
        ser_json = _json.dumps(req.to_dict())
        deser_dict = _json.loads(ser_json)
        self.assertEqual("test_user", deser_dict["name"])

    def test_user_add_request_validate(self):
        req = UserAddRequest("test_user")
        req.validate()  # should not raise

    def test_user_add_request_validate_empty(self):
        req = UserAddRequest("")
        with self.assertRaises(ValueError):
            req.validate()


class TestGroupAddRequest(unittest.TestCase):
    def test_group_add_request_serde(self):
        req = GroupAddRequest("test_group")
        ser_json = _json.dumps(req.to_dict())
        deser_dict = _json.loads(ser_json)
        self.assertEqual("test_group", deser_dict["name"])

    def test_group_add_request_validate(self):
        req = GroupAddRequest("test_group")
        req.validate()

    def test_group_add_request_validate_empty(self):
        req = GroupAddRequest("")
        with self.assertRaises(ValueError):
            req.validate()


class TestRoleCreateRequest(unittest.TestCase):
    def test_role_create_request_serde_minimal(self):
        req = RoleCreateRequest("test_role", None, [])
        ser_json = _json.dumps(req.to_dict())
        deser_dict = _json.loads(ser_json)
        self.assertEqual("test_role", deser_dict["name"])
        self.assertIsNone(deser_dict.get("properties"))
        self.assertEqual(deser_dict["securableObjects"], [])

    def test_role_create_request_serde_full(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.CREATE_FILESET,
            _condition=Privilege.Condition.ALLOW,
        )
        securable_obj = SecurableObjectDTO(
            _full_name="test_metalake",
            _type=MetadataObject.Type.METALAKE,
            _privileges=[privilege],
        )
        req = RoleCreateRequest(
            "test_role",
            {"purpose": "test"},
            [securable_obj],
        )
        d = req.to_dict()
        self.assertEqual("test_role", d["name"])
        self.assertEqual(d["properties"], {"purpose": "test"})
        self.assertEqual(len(d["securableObjects"]), 1)
        self.assertEqual(
            d["securableObjects"][0]["fullName"], "test_metalake"
        )

    def test_role_create_request_validate(self):
        req = RoleCreateRequest("test_role", None, [])
        req.validate()

    def test_role_create_request_validate_empty(self):
        req = RoleCreateRequest("", None, [])
        with self.assertRaises(ValueError):
            req.validate()


class TestRoleGrantRequest(unittest.TestCase):
    def test_role_grant_request_serde(self):
        req = RoleGrantRequest(["role1", "role2"])
        ser_json = _json.dumps(req.to_dict())
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["roleNames"], ["role1", "role2"])

    def test_role_grant_request_validate(self):
        req = RoleGrantRequest(["role1"])
        req.validate()

    def test_role_grant_request_validate_empty(self):
        req = RoleGrantRequest([])
        with self.assertRaises(ValueError):
            req.validate()


class TestRoleRevokeRequest(unittest.TestCase):
    def test_role_revoke_request_serde(self):
        req = RoleRevokeRequest(["role1", "role2"])
        ser_json = _json.dumps(req.to_dict())
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["roleNames"], ["role1", "role2"])

    def test_role_revoke_request_validate(self):
        req = RoleRevokeRequest(["role1"])
        req.validate()

    def test_role_revoke_request_validate_empty(self):
        req = RoleRevokeRequest([])
        with self.assertRaises(ValueError):
            req.validate()


class TestPrivilegeGrantRequest(unittest.TestCase):
    def test_privilege_grant_request_serde(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )
        req = PrivilegeGrantRequest([privilege])
        d = req.to_dict()
        self.assertEqual(len(d["privileges"]), 1)
        self.assertEqual(
            d["privileges"][0]["name"], "use_catalog"
        )
        self.assertEqual(
            d["privileges"][0]["condition"], "allow"
        )

    def test_privilege_grant_request_validate(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )
        req = PrivilegeGrantRequest([privilege])
        req.validate()

    def test_privilege_grant_request_validate_empty(self):
        req = PrivilegeGrantRequest([])
        with self.assertRaises(ValueError):
            req.validate()


class TestPrivilegeRevokeRequest(unittest.TestCase):
    def test_privilege_revoke_request_serde(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )
        req = PrivilegeRevokeRequest([privilege])
        d = req.to_dict()
        self.assertEqual(len(d["privileges"]), 1)

    def test_privilege_revoke_request_validate(self):
        privilege = PrivilegeDTO(
            _name=Privilege.Name.USE_CATALOG,
            _condition=Privilege.Condition.ALLOW,
        )
        req = PrivilegeRevokeRequest([privilege])
        req.validate()

    def test_privilege_revoke_request_validate_empty(self):
        req = PrivilegeRevokeRequest([])
        with self.assertRaises(ValueError):
            req.validate()
