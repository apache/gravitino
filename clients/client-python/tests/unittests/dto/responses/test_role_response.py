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

from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.responses.role_response import RoleNamesListResponse, RoleResponse


class TestRoleResponses(unittest.TestCase):
    def test_role_response(self):
        role_dto = (
            RoleDTO.builder()
            .with_name("role1")
            .with_properties({"purpose": "test"})
            .build()
        )
        resp = RoleResponse(0, role_dto)

        resp.validate()

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(role_dto, resp.role())
        self.assertEqual(0, deser_dict["code"])
        self.assertIsNotNone(deser_dict.get("role"))
        self.assertEqual("role1", deser_dict["role"]["name"])
        self.assertEqual(
            deser_dict["role"]["properties"], {"purpose": "test"}
        )

    def test_role_response_validate_no_role(self):
        resp = RoleResponse(0, None)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_role_names_list_response(self):
        names = ["role1", "role2", "role3"]
        resp = RoleNamesListResponse(0, names)

        resp.validate()

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertEqual(3, len(deser_dict["names"]))
        self.assertListEqual(names, deser_dict["names"])
