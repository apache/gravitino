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

from gravitino.dto.authorization.user_dto import UserDTO
from gravitino.dto.responses.user_response import (
    UserListResponse,
    UserNamesListResponse,
    UserResponse,
)


class TestUserResponses(unittest.TestCase):
    def test_user_response(self):
        user_dto = UserDTO.builder().with_name("user1").build()
        resp = UserResponse(0, user_dto)

        resp.validate()

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(user_dto, resp.user())
        self.assertEqual(0, deser_dict["code"])
        self.assertIsNotNone(deser_dict.get("user"))
        self.assertEqual("user1", deser_dict["user"]["name"])

    def test_user_response_validate_no_user(self):
        resp = UserResponse(0, None)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_user_names_list_response(self):
        names = ["user1", "user2", "user3"]
        resp = UserNamesListResponse(0, names)

        resp.validate()

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertEqual(3, len(deser_dict["names"]))
        self.assertListEqual(names, deser_dict["names"])

    def test_user_list_response(self):
        user1 = UserDTO.builder().with_name("user1").with_roles(["r1"]).build()
        user2 = UserDTO.builder().with_name("user2").build()

        resp = UserListResponse(0, [user1, user2])

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertEqual(2, len(deser_dict["users"]))
        self.assertEqual("user1", deser_dict["users"][0]["name"])
        self.assertEqual(["r1"], deser_dict["users"][0]["roles"])
        self.assertEqual("user2", deser_dict["users"][1]["name"])
