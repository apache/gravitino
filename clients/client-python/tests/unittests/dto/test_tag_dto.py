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
from gravitino.dto.tag_dto import TagDTO


class TestTagDTO(unittest.TestCase):
    def test_create_tag_dto(self):
        builder = TagDTO.builder()
        tag_dto = (
            builder.name("test_tag")
            .comment("test_comment")
            .properties(
                {
                    "key1": "value1",
                    "key2": "value2",
                }
            )
            .audit_info(AuditDTO("test_user", 1640995200000))
            .inherited(True)
            .build()
        )
        ser_json = _json.dumps(tag_dto.to_dict()).encode("utf-8")
        deser_dict = _json.loads(ser_json)
        self.assertEqual(deser_dict["name"], "test_tag")
        self.assertEqual(deser_dict["comment"], "test_comment")
        self.assertEqual(deser_dict["properties"], {"key1": "value1", "key2": "value2"})
        self.assertTrue(deser_dict["inherited"])
        self.assertEqual(deser_dict["audit"]["creator"], "test_user")
        self.assertEqual(deser_dict["audit"]["createTime"], 1640995200000)

    def test_equality_and_hash(self):
        builder = TagDTO.builder()
        tag_dto1 = (
            builder.name("test_tag")
            .comment("test_comment")
            .properties(
                {
                    "key1": "value1",
                    "key2": "value2",
                }
            )
            .audit_info(AuditDTO("test_user", 1640995200000))
            .inherited(True)
            .build()
        )
        tag_dto2 = (
            builder.name("test_tag")
            .comment("test_comment")
            .properties(
                {
                    "key1": "value1",
                    "key2": "value2",
                }
            )
            .audit_info(AuditDTO("test_user", 1640995200000))
            .inherited(True)
            .build()
        )
        tag_dto3 = (
            builder.name("test_tag")
            .comment("test_comment")
            .properties(
                {
                    "key1": "value2",
                    "key2": "value3",
                }
            )
            .audit_info(AuditDTO("test_user", 1640995200000))
            .inherited(False)
            .build()
        )

        self.assertEqual(tag_dto1, tag_dto2)
        self.assertEqual(hash(tag_dto1), hash(tag_dto2))

        self.assertNotEqual(tag_dto1, tag_dto3)
        self.assertNotEqual(hash(tag_dto1), hash(tag_dto3))

        self.assertNotEqual(tag_dto2, tag_dto3)
        self.assertNotEqual(hash(tag_dto2), hash(tag_dto3))
