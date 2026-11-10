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

from gravitino.api.tag.tag_change import TagChange
from gravitino.client.dto_converters import DTOConverters
from gravitino.dto.requests.tag_update_request import TagUpdateRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestDtoConverters(unittest.TestCase):
    def test_to_tag_update_request_with_name(self) -> None:
        new_tag_name = "new_tag_name"
        change = TagChange.rename(new_tag_name)
        rename_request = DTOConverters.to_tag_update_request(change)

        self.assertTrue(isinstance(rename_request, TagUpdateRequest.RenameTagRequest))
        self.assertEqual(new_tag_name, rename_request.new_name)

        json_str = _json.dumps(
            {
                "@type": "rename",
                "newName": f"{new_tag_name}",
            }
        )
        self.assertEqual(json_str, rename_request.to_json())

    def test_to_tag_update_request_with_comment(self) -> None:
        new_comment = "new_comment"
        change = TagChange.update_comment(new_comment)
        update_comment_request = DTOConverters.to_tag_update_request(change)

        self.assertTrue(
            isinstance(update_comment_request, TagUpdateRequest.UpdateTagCommentRequest)
        )

        json_str = _json.dumps(
            {
                "@type": "updateComment",
                "newComment": f"{new_comment}",
            }
        )
        self.assertEqual(json_str, update_comment_request.to_json())

    def test_to_tag_update_request_with_new_property(self) -> None:
        new_prop = "key"
        new_value = "value"
        change = TagChange.set_property(new_prop, new_value)
        set_property_request = DTOConverters.to_tag_update_request(change)

        self.assertTrue(
            isinstance(set_property_request, TagUpdateRequest.SetTagPropertyRequest)
        )
        json_str = _json.dumps(
            {
                "@type": "setProperty",
                "property": f"{new_prop}",
                "value": f"{new_value}",
            }
        )
        self.assertEqual(json_str, set_property_request.to_json())

    def test_to_tag_update_request_with_remove_property(self) -> None:
        removed_prop = "key"
        change = TagChange.remove_property(removed_prop)
        remove_property_request = DTOConverters.to_tag_update_request(change)

        self.assertTrue(
            isinstance(
                remove_property_request, TagUpdateRequest.RemoveTagPropertyRequest
            )
        )
        json_str = _json.dumps(
            {
                "@type": "removeProperty",
                "property": f"{removed_prop}",
            }
        )
        self.assertEqual(json_str, remove_property_request.to_json())

    def test_to_tag_update_request_with_unsupport_type(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            DTOConverters.to_tag_update_request(None)
