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

from gravitino.api.tag.tag_change import TagChange
from gravitino.dto.requests.tag_update_request import TagUpdateRequest


class TestTagUpdateRequest(unittest.TestCase):
    def test_tag_rename_tag_request_validate(self) -> None:
        invalid_request = TagUpdateRequest.RenameTagRequest(_new_name="")
        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_tag_rename_tag_request_serde(self) -> None:
        request = TagUpdateRequest.RenameTagRequest(_new_name="new_name")
        json_str = _json.dumps(
            {
                "@type": "rename",
                "newName": "new_name",
            }
        )
        self.assertEqual(json_str, request.to_json())

        deserialized_request = TagUpdateRequest.RenameTagRequest.from_json(json_str)
        self.assertEqual("new_name", deserialized_request.new_name())
        self.assertIsInstance(deserialized_request.tag_change(), TagChange.RenameTag)

    def test_update_tag_comment_request_serde(self) -> None:
        request = TagUpdateRequest.UpdateTagCommentRequest("new_comment")
        json_str = _json.dumps(
            {
                "@type": "updateComment",
                "newComment": "new_comment",
            }
        )
        self.assertEqual(json_str, request.to_json())

        deserialized_request = TagUpdateRequest.UpdateTagCommentRequest.from_json(
            json_str
        )
        self.assertEqual("new_comment", deserialized_request.new_comment())
        self.assertIsInstance(
            deserialized_request.tag_change(), TagChange.UpdateTagComment
        )

    def test_set_tag_property_request_validate(self) -> None:
        invalid_request = TagUpdateRequest.SetTagPropertyRequest("key", "")
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TagUpdateRequest.SetTagPropertyRequest("", "value")
        with self.assertRaises(ValueError):
            invalid_request.validate()

        invalid_request = TagUpdateRequest.SetTagPropertyRequest("", "")
        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_set_tag_property_request_serde(self) -> None:
        request = TagUpdateRequest.SetTagPropertyRequest("key", "value")
        json_str = _json.dumps(
            {
                "@type": "setProperty",
                "property": "key",
                "value": "value",
            }
        )
        self.assertEqual(json_str, request.to_json())

        deserialized_request = TagUpdateRequest.SetTagPropertyRequest.from_json(
            json_str
        )
        self.assertEqual("key", deserialized_request.get_prop())
        self.assertEqual("value", deserialized_request.get_value())
        self.assertIsInstance(deserialized_request.tag_change(), TagChange.SetProperty)

    def test_remove_tag_property_request_validate(self) -> None:
        invalid_request = TagUpdateRequest.RemoveTagPropertyRequest("")
        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_remove_tag_property_request_serde(self) -> None:
        request = TagUpdateRequest.RemoveTagPropertyRequest("key")
        json_str = _json.dumps(
            {
                "@type": "removeProperty",
                "property": "key",
            }
        )
        self.assertEqual(json_str, request.to_json())

        deserialized_request = TagUpdateRequest.RemoveTagPropertyRequest.from_json(
            json_str
        )
        self.assertEqual("key", deserialized_request.get_prop())
        self.assertIsInstance(
            deserialized_request.tag_change(), TagChange.RemoveProperty
        )
