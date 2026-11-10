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

from gravitino.dto.responses.policy_response import (
    MetadataObjectPolicyListResponse,
    PolicyForTagAssociationListResponse,
    PolicyListResponse,
    PolicyNameListResponse,
    PolicyResponse,
    PolicyTagAssociationResponse,
    TagForPolicyAssociationListResponse,
)
from gravitino.exceptions.base import IllegalArgumentException

_POLICY = {
    "name": "policy1",
    "policyType": "system_access_control",
    "enabled": True,
    "content": {"privileges": ["select_table"], "applicableRoles": ["analyst"]},
}


class TestPolicyResponse(unittest.TestCase):
    def test_policy_response(self) -> None:
        response = PolicyResponse.from_json(
            json.dumps({"code": 0, "policy": _POLICY}), infer_missing=True
        )
        response.validate()
        self.assertEqual(0, response.code())
        self.assertEqual("policy1", response.policy().name())
        round_tripped = PolicyResponse.from_json(response.to_json(), infer_missing=True)
        self.assertEqual(response.policy(), round_tripped.policy())

    def test_policy_response_null_policy_invalid(self) -> None:
        response = PolicyResponse.from_json(json.dumps({"code": 0}), infer_missing=True)
        with self.assertRaises(IllegalArgumentException):
            response.validate()


class TestPolicyListResponse(unittest.TestCase):
    def test_policy_list_response(self) -> None:
        response = PolicyListResponse.from_json(
            json.dumps({"code": 0, "policies": [_POLICY, _POLICY]}),
            infer_missing=True,
        )
        response.validate()
        self.assertEqual(2, len(response.policies()))
        self.assertEqual("policy1", response.policies()[0].name())

    def test_policy_list_response_null_invalid(self) -> None:
        response = PolicyListResponse.from_json(
            json.dumps({"code": 0}), infer_missing=True
        )
        with self.assertRaises(IllegalArgumentException):
            response.validate()

    def test_policy_name_list_response(self) -> None:
        response = PolicyNameListResponse.from_json(
            json.dumps(
                {
                    "code": 0,
                    "names": ["policy1", "policy2"],
                    "hasInaccessiblePolicies": True,
                }
            ),
            infer_missing=True,
        )
        response.validate()
        self.assertEqual(["policy1", "policy2"], response.names)
        self.assertTrue(response.has_inaccessible_policies())

    def test_policy_name_list_response_default_visibility(self) -> None:
        response = PolicyNameListResponse.from_json(
            json.dumps({"code": 0, "names": []}), infer_missing=True
        )
        self.assertFalse(response.has_inaccessible_policies())

    def test_metadata_object_policy_list_response(self) -> None:
        response = MetadataObjectPolicyListResponse.from_json(
            json.dumps(
                {
                    "code": 0,
                    "policies": [_POLICY],
                    "hasInaccessiblePolicies": True,
                }
            ),
            infer_missing=True,
        )
        response.validate()
        self.assertEqual(1, len(response.policies()))
        self.assertTrue(response.has_inaccessible_policies())


class TestPolicyTagAssociationResponse(unittest.TestCase):
    def test_association_response(self) -> None:
        response = PolicyTagAssociationResponse.from_json(
            json.dumps(
                {
                    "code": 0,
                    "policy": "policy1",
                    "tag": "tag1",
                    "selector": {"type": "TAG_VALUE", "value": "gold"},
                }
            ),
            infer_missing=True,
        )
        response.validate()
        self.assertEqual("policy1", response.policy())
        self.assertEqual("tag1", response.tag())
        self.assertEqual("gold", response.selector().value())

    def test_association_response_invalid(self) -> None:
        response = PolicyTagAssociationResponse.from_json(
            json.dumps(
                {
                    "code": 0,
                    "policy": "policy1",
                    "tag": "tag1",
                    "selector": {"type": "ALL_VALUES", "value": "gold"},
                }
            ),
            infer_missing=True,
        )
        with self.assertRaises(IllegalArgumentException):
            response.validate()


class TestAssociationListResponses(unittest.TestCase):
    def test_policy_for_tag_association_list_response(self) -> None:
        response = PolicyForTagAssociationListResponse.from_json(
            json.dumps(
                {
                    "code": 0,
                    "associations": [
                        {
                            "policy": _POLICY,
                            "selector": {"type": "ALL_VALUES"},
                        }
                    ],
                }
            ),
            infer_missing=True,
        )
        response.validate()
        associations = response.associations()
        self.assertEqual(1, len(associations))
        self.assertEqual("policy1", associations[0].policy().name())
        self.assertEqual("ALL_VALUES", associations[0].selector().type())

    def test_tag_for_policy_association_list_response(self) -> None:
        response = TagForPolicyAssociationListResponse.from_json(
            json.dumps(
                {
                    "code": 0,
                    "associations": [
                        {
                            "tag": {"name": "tag1", "properties": {}},
                            "selector": {"type": "ALL_VALUES"},
                        }
                    ],
                }
            ),
            infer_missing=True,
        )
        response.validate()
        associations = response.associations()
        self.assertEqual(1, len(associations))
        self.assertEqual("tag1", associations[0].tag().name())
        self.assertEqual("ALL_VALUES", associations[0].selector().type())


if __name__ == "__main__":
    unittest.main()
