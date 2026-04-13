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

import json as _json
import unittest

from gravitino.dto.responses.role_response import RoleNamesListResponse
from gravitino.exceptions.base import IllegalArgumentException


class TestRoleResponses(unittest.TestCase):
    def test_role_names_list_response(self) -> None:
        role_response = RoleNamesListResponse(0, ["role1", "role2", "role3"])
        role_response.validate()

        ser_json = _json.dumps(role_response.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict.get("code"))
        self.assertIsNotNone(deser_dict.get("names"))
        self.assertEqual(3, len(deser_dict.get("names")))
        self.assertEqual("role1", deser_dict["names"][0])
        self.assertEqual("role2", deser_dict["names"][1])
        self.assertEqual("role3", deser_dict["names"][2])

    def test_role_names_list_response_validation(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            role_response = RoleNamesListResponse(0, None)  # type: ignore
            role_response.validate()

        with self.assertRaises(IllegalArgumentException):
            role_response = RoleNamesListResponse(0, ["role1", ""])
            role_response.validate()
