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

from gravitino.dto.requests.tag_associate_request import TagsAssociateRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestTagsAssociateRequest(unittest.TestCase):
    def test_create_request(self) -> None:
        request = TagsAssociateRequest(
            ["tag_to_add_1", "tag_to_add_2"], ["tag_to_remove_1", "tag_to_remove_2"]
        )
        json_str = _json.dumps(
            {
                "tagsToAdd": ["tag_to_add_1", "tag_to_add_2"],
                "tagsToRemove": ["tag_to_remove_1", "tag_to_remove_2"],
            }
        )

        self.assertEqual(json_str, request.to_json())
        deserialized_request = TagsAssociateRequest.from_json(json_str)

        self.assertTrue(isinstance(deserialized_request, TagsAssociateRequest))
        self.assertEqual(
            ["tag_to_add_1", "tag_to_add_2"], deserialized_request.tags_to_add
        )
        self.assertEqual(
            ["tag_to_remove_1", "tag_to_remove_2"], deserialized_request.tags_to_remove
        )

    def test_associaate_request_validate(self) -> None:
        invalid_request1 = TagsAssociateRequest(
            None, None
        )  # pyright: ignore[reportArgumentType]
        invalid_request2 = TagsAssociateRequest(
            ["tag_to_add_1", " "], ["tag_to_remove_1", "tag_to_remove_2"]
        )

        with self.assertRaises(IllegalArgumentException):
            invalid_request1.validate()

        with self.assertRaises(IllegalArgumentException):
            invalid_request2.validate()
