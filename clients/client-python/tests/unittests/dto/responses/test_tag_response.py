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

from gravitino.dto.responses.tag_response import (
    TagListResponse,
    TagNamesListResponse,
    TagResponse,
)
from gravitino.dto.tag_dto import TagDTO


class TestTagResponses(unittest.TestCase):
    def test_tag_response(self) -> None:
        tag_dto = TagDTO.builder().name("tag1").comment("comment1").build()
        tag_resp = TagResponse(0, tag_dto)

        tag_resp.validate()

        # test serialization
        ser_json = _json.dumps(tag_resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(tag_dto, tag_resp.tag())
        self.assertEqual(0, deser_dict["code"])
        self.assertIsNotNone(deser_dict.get("tag"))
        self.assertEqual("tag1", deser_dict["tag"]["name"])
        self.assertEqual("comment1", deser_dict["tag"]["comment"])

        tag_dto_invalid = TagDTO.builder().build()
        with self.assertRaises(ValueError):
            tag_resp = TagResponse(tag_dto_invalid)
            tag_resp.validate()

    def test_tag_name_list_response(self) -> None:
        tag_names = ["tag1", "tag2", "tag3"]
        tag_name_list_resp = TagNamesListResponse(0, tag_names)

        tag_name_list_resp.validate()

        # test serialization
        ser_json = _json.dumps(tag_name_list_resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertIsNotNone(deser_dict.get("names"))
        self.assertEqual(3, len(deser_dict["names"]))
        self.assertListEqual(tag_names, deser_dict["names"])

    def test_tag_list_response(self) -> None:
        tag_dto1 = TagDTO.builder().name("tag1").comment("comment1").build()
        tag_dto2 = TagDTO.builder().name("tag2").comment("comment2").build()
        tag_dto3 = TagDTO.builder().name("tag3").comment("comment3").build()

        tag_list_resp = TagListResponse(0, [tag_dto1, tag_dto2, tag_dto3])

        tag_list_resp.validate()

        # test serialization
        ser_json = _json.dumps(tag_list_resp.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual(0, deser_dict["code"])
        self.assertIsNotNone(deser_dict.get("tags"))
        self.assertEqual(3, len(deser_dict["tags"]))

        self.assertEqual("tag1", deser_dict["tags"][0]["name"])
        self.assertEqual("comment1", deser_dict["tags"][0]["comment"])

        self.assertEqual("tag2", deser_dict["tags"][1]["name"])
        self.assertEqual("comment2", deser_dict["tags"][1]["comment"])

        self.assertEqual("tag3", deser_dict["tags"][2]["name"])
        self.assertEqual("comment3", deser_dict["tags"][2]["comment"])
