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

import json
import unittest

from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.dto.policy_dto import (
    PolicyAssociationSelectorDTO,
    RowFilterContentDTO,
)
from gravitino.dto.requests.policy_create_request import PolicyCreateRequest
from gravitino.dto.requests.policy_set_request import PolicySetRequest
from gravitino.dto.requests.policy_tag_add_request import PolicyTagAddRequest
from gravitino.dto.requests.policy_update_request import PolicyUpdateRequest
from gravitino.dto.requests.policy_updates_request import PolicyUpdatesRequest
from gravitino.exceptions.base import IllegalArgumentException


def _row_filter_content() -> RowFilterContentDTO:
    return RowFilterContentDTO.from_dict({"expression": "age > 18"}, infer_missing=True)


class TestPolicyCreateRequest(unittest.TestCase):
    def test_serialization_and_validation(self) -> None:
        request = PolicyCreateRequest(
            _name="policy1",
            _policy_type="system_row_filter",
            _comment="a comment",
            _enabled=False,
            _content=_row_filter_content(),
        )
        request.validate()
        wire = json.loads(request.to_json())
        self.assertEqual("policy1", wire["name"])
        self.assertEqual("system_row_filter", wire["policyType"])
        self.assertEqual("a comment", wire["comment"])
        self.assertEqual(False, wire["enabled"])
        self.assertEqual({"expression": "age > 18"}, wire["content"])

    def test_default_enabled(self) -> None:
        request = PolicyCreateRequest(
            _name="policy1",
            _policy_type="system_row_filter",
            _content=_row_filter_content(),
        )
        self.assertTrue(request.enabled)
        self.assertTrue(json.loads(request.to_json())["enabled"])

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyCreateRequest(
                _name=" ",
                _policy_type="system_row_filter",
                _content=_row_filter_content(),
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyCreateRequest(
                _name="p", _policy_type="", _content=_row_filter_content()
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyCreateRequest(
                _name="p", _policy_type="system_row_filter", _content=None
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyCreateRequest(
                _name="p",
                _policy_type="system_row_filter",
                _content=RowFilterContentDTO.from_dict(
                    {"expression": ""}, infer_missing=True
                ),
            ).validate()


class TestPolicyUpdateRequest(unittest.TestCase):
    def test_rename_request(self) -> None:
        request = PolicyUpdateRequest.RenamePolicyRequest("new_name")
        request.validate()
        wire = json.loads(request.to_json())
        self.assertEqual("rename", wire["@type"])
        self.assertEqual("new_name", wire["newName"])
        self.assertEqual("new_name", request.policy_change().new_name)
        with self.assertRaises(IllegalArgumentException):
            PolicyUpdateRequest.RenamePolicyRequest(" ").validate()

    def test_update_comment_request(self) -> None:
        request = PolicyUpdateRequest.UpdatePolicyCommentRequest("new comment")
        request.validate()
        wire = json.loads(request.to_json())
        self.assertEqual("updateComment", wire["@type"])
        self.assertEqual("new comment", wire["newComment"])
        self.assertEqual("new comment", request.policy_change().new_comment)

    def test_update_content_request(self) -> None:
        request = PolicyUpdateRequest.UpdatePolicyContentRequest(
            "system_row_filter", _row_filter_content()
        )
        request.validate()
        wire = json.loads(request.to_json())
        self.assertEqual("updateContent", wire["@type"])
        self.assertEqual("system_row_filter", wire["policyType"])
        self.assertEqual({"expression": "age > 18"}, wire["newContent"])
        change = request.policy_change()
        self.assertEqual("system_row_filter", change.policy_type)
        self.assertEqual(PolicyContents.row_filter("age > 18"), change.content)
        with self.assertRaises(IllegalArgumentException):
            PolicyUpdateRequest.UpdatePolicyContentRequest(
                " ", _row_filter_content()
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyUpdateRequest.UpdatePolicyContentRequest(
                "system_row_filter", None
            ).validate()


class TestPolicyUpdatesRequest(unittest.TestCase):
    def test_serialization_and_validation(self) -> None:
        request = PolicyUpdatesRequest(
            [
                PolicyUpdateRequest.RenamePolicyRequest("new_name"),
                PolicyUpdateRequest.UpdatePolicyCommentRequest("c"),
            ]
        )
        request.validate()
        wire = json.loads(request.to_json())
        self.assertEqual(2, len(wire["updates"]))
        self.assertEqual("rename", wire["updates"][0]["@type"])
        self.assertEqual("updateComment", wire["updates"][1]["@type"])

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyUpdatesRequest(None).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyUpdatesRequest(
                [PolicyUpdateRequest.RenamePolicyRequest(" ")]
            ).validate()


class TestPolicySetRequest(unittest.TestCase):
    def test_serialization(self) -> None:
        self.assertEqual(
            {"enable": False}, json.loads(PolicySetRequest(False).to_json())
        )
        self.assertEqual({"enable": True}, json.loads(PolicySetRequest(True).to_json()))
        PolicySetRequest(True).validate()


class TestPolicyTagAddRequest(unittest.TestCase):
    def test_serialization_and_selector(self) -> None:
        request = PolicyTagAddRequest(PolicyAssociationSelectorDTO("TAG_VALUE", "gold"))
        request.validate()
        wire = json.loads(request.to_json())
        self.assertEqual({"type": "TAG_VALUE", "value": "gold"}, wire["selector"])
        selector = request.selector()
        self.assertEqual("TAG_VALUE", selector.type())
        self.assertEqual("gold", selector.value())

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyTagAddRequest(None).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyTagAddRequest(
                PolicyAssociationSelectorDTO("ALL_VALUES", "gold")
            ).validate()


if __name__ == "__main__":
    unittest.main()
