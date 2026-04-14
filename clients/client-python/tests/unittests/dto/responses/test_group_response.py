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

from gravitino.dto.authorization.group_dto import GroupDTO
from gravitino.dto.responses.group_response import (
    GroupListResponse,
    GroupNamesListResponse,
    GroupResponse,
)


class TestGroupResponses(unittest.TestCase):
    def test_group_response(self):
        group_dto = GroupDTO.builder().with_name("group1").build()
        resp = GroupResponse(0, group_dto)

        resp.validate()

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(group_dto, resp.group())
        self.assertEqual(0, deser_dict["code"])
        self.assertIsNotNone(deser_dict.get("group"))
        self.assertEqual("group1", deser_dict["group"]["name"])

    def test_group_response_validate_no_group(self):
        resp = GroupResponse(0, None)
        with self.assertRaises(ValueError):
            resp.validate()

    def test_group_names_list_response(self):
        names = ["group1", "group2"]
        resp = GroupNamesListResponse(0, names)

        resp.validate()

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertEqual(2, len(deser_dict["names"]))
        self.assertListEqual(names, deser_dict["names"])

    def test_group_list_response(self):
        group1 = GroupDTO.builder().with_name("group1").with_roles(["r1"]).build()
        group2 = GroupDTO.builder().with_name("group2").build()

        resp = GroupListResponse(0, [group1, group2])

        ser_json = _json.dumps(resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertEqual(2, len(deser_dict["groups"]))
        self.assertEqual("group1", deser_dict["groups"][0]["name"])
        self.assertEqual("group2", deser_dict["groups"][1]["name"])
