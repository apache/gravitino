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

import unittest
from unittest.mock import patch

from gravitino import GravitinoClient
from gravitino.api.authorization.privileges import Privilege
from gravitino.api.policy.policy_association_selector import TagValueSelector
from gravitino.api.policy.policy_change import PolicyChange
from gravitino.api.policy.policy_content import AccessControlContent
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.api.tag.tag_operations import TagOperations
from gravitino.client.generic_policy import GenericPolicy
from gravitino.dto.policy_dto import (
    PolicyForTagAssociationDTO,
    PolicyAssociationSelectorDTO,
    TagForPolicyAssociationDTO,
)
from gravitino.dto.requests.policy_set_request import PolicySetRequest
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.policy_response import (
    PolicyForTagAssociationListResponse,
    PolicyListResponse,
    PolicyNameListResponse,
    PolicyResponse,
    PolicyTagAssociationResponse,
    TagForPolicyAssociationListResponse,
)
from gravitino.dto.responses.tag_response import TagResponse
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.policy_error_handler import POLICY_ERROR_HANDLER
from gravitino.exceptions.handlers.tag_error_handler import TAG_ERROR_HANDLER
from tests.unittests import mock_base


@mock_base.mock_data
class TestPolicyClientAPI(unittest.TestCase):
    _metalake_name: str = "metalake_demo"

    def _client(self) -> GravitinoClient:
        return GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

    def check_policy_equal(self, expected, actual) -> None:
        self.assertEqual(expected.name(), actual.name())
        self.assertEqual(expected.policy_type(), actual.policy_type())
        self.assertEqual(expected.comment(), actual.comment())
        self.assertEqual(expected.enabled(), actual.enabled())

    # Client delegation tests, backed by the mock policy repo.

    def test_client_list_policies(self, *mock_method) -> None:
        with mock_base.mock_policy_methods():
            client = self._client()
            self.assertEqual(["policyA", "policyB"], client.list_policies())
            self.assertEqual(2, len(client.list_policy_infos()))

    def test_client_get_policy(self, *mock_method) -> None:
        with mock_base.mock_policy_methods():
            client = self._client()
            policy = client.get_policy("policyA")
            self.assertEqual("policyA", policy.name())
            self.assertEqual("mock policy A", policy.comment())

    def test_client_create_policy(self, *mock_method) -> None:
        with mock_base.mock_policy_methods():
            client = self._client()
            created = client.create_policy(
                "policyC",
                "system_access_control",
                "mock policy C",
                True,
                PolicyContents.access_control(
                    [Privilege.Name.SELECT_TABLE], ["analyst"]
                ),
            )
            self.check_policy_equal(
                mock_base.build_policy_dto("policyC", "mock policy C"), created
            )
            self.assertEqual(["policyA", "policyB", "policyC"], client.list_policies())

    def test_client_alter_policy(self, *mock_method) -> None:
        with mock_base.mock_policy_methods():
            client = self._client()
            client.alter_policy(
                "policyB",
                PolicyChange.update_comment("mock policy B updated"),
                PolicyChange.rename("new_policy_B"),
            )
            retrieved = client.get_policy("new_policy_B")
            self.assertEqual("new_policy_B", retrieved.name())
            self.assertEqual("mock policy B updated", retrieved.comment())

    def test_client_enable_disable_policy(self, *mock_method) -> None:
        with mock_base.mock_policy_methods():
            client = self._client()
            client.disable_policy("policyA")
            self.assertFalse(client.get_policy("policyA").enabled())
            client.enable_policy("policyA")
            self.assertTrue(client.get_policy("policyA").enabled())

    def test_client_delete_policy(self, *mock_method) -> None:
        with mock_base.mock_policy_methods():
            client = self._client()
            self.assertTrue(client.delete_policy("policyA"))
            self.assertFalse(client.delete_policy("policyA"))
            self.assertEqual(["policyB"], client.list_policies())

    def test_tag_operations_policy_defaults_unsupported(self, *mock_method) -> None:
        class BareTagOperations(TagOperations):
            def list_tags(self):
                return []

            def list_tags_info(self):
                return []

            def get_tag(self, tag_name):
                return None

            def create_tag(self, tag_name, comment, properties, allowed_values=None):
                return None

            def alter_tag(self, tag_name, *changes):
                return None

            def delete_tag(self, tag_name):
                return False

        operations = BareTagOperations()
        with self.assertRaises(UnsupportedOperationException):
            operations.list_policy_associations_for_tag("tagA")
        with self.assertRaises(UnsupportedOperationException):
            operations.add_policy_for_tag("tagA", "policyA")
        with self.assertRaises(UnsupportedOperationException):
            operations.remove_policy_from_tag("tagA", "policyA")

    # Metalake HTTP-level tests, with the HTTP client verbs mocked.

    def test_gravitino_list_policies_api(self, *mock_method) -> None:
        resp = PolicyNameListResponse(0, ["policyA", "policyB"])
        mock_resp = mock_base.mock_http_response(resp.to_json())
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            policies = client.list_policies()
            self.assertEqual(["policyA", "policyB"], policies)
            mock_get.assert_called_once_with(
                "api/metalakes/metalake_demo/policies",
                error_handler=POLICY_ERROR_HANDLER,
            )

    def test_gravitino_list_policy_infos_api(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        resp = PolicyListResponse(0, [policy])
        mock_resp = mock_base.mock_http_response(resp.to_json())
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            policies = client.list_policy_infos()
            self.assertEqual(1, len(policies))
            self.assertIsInstance(policies[0], GenericPolicy)
            self.check_policy_equal(policy, policies[0])
            self.assertIsInstance(policies[0].content(), AccessControlContent)
            mock_get.assert_called_once_with(
                "api/metalakes/metalake_demo/policies",
                params={"details": "true"},
                error_handler=POLICY_ERROR_HANDLER,
            )

    def test_gravitino_get_policy_api(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        resp = PolicyResponse(0, policy)
        mock_resp = mock_base.mock_http_response(resp.to_json())
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            retrieved = client.get_policy(policy.name())
            self.check_policy_equal(policy, retrieved)

    def test_gravitino_get_policy_api_with_empty_name(self, *mock_method) -> None:
        client = self._client()
        with self.assertRaises(IllegalArgumentException):
            client.get_policy(" ")

    def test_gravitino_create_policy_api(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        resp = PolicyResponse(0, policy)
        mock_resp = mock_base.mock_http_response(resp.to_json())
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ) as mock_post:
            created = client.create_policy(
                policy.name(),
                policy.policy_type(),
                policy.comment(),
                policy.enabled(),
                PolicyContents.access_control(
                    [Privilege.Name.SELECT_TABLE], ["analyst"]
                ),
            )
            self.check_policy_equal(policy, created)
            args, kwargs = mock_post.call_args
            self.assertEqual("api/metalakes/metalake_demo/policies", args[0])
            self.assertEqual(POLICY_ERROR_HANDLER, kwargs["error_handler"])
            request = kwargs["json"]
            self.assertEqual("policyA", request.name)
            self.assertEqual("system_access_control", request.policy_type)

    def test_gravitino_enable_disable_policy_api(self, *mock_method) -> None:
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.patch",
        ) as mock_patch:
            client.enable_policy("policyA")
            args, kwargs = mock_patch.call_args
            self.assertEqual("api/metalakes/metalake_demo/policies/policyA", args[0])
            self.assertIsInstance(kwargs["json"], PolicySetRequest)
            self.assertTrue(kwargs["json"].enable)
            self.assertEqual(POLICY_ERROR_HANDLER, kwargs["error_handler"])

        with patch(
            "gravitino.utils.http_client.HTTPClient.patch",
        ) as mock_patch:
            client.disable_policy("policyA")
            _, kwargs = mock_patch.call_args
            self.assertFalse(kwargs["json"].enable)

    def test_gravitino_alter_policy_api(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto("new_policy", "new comment")
        resp = PolicyResponse(0, policy)
        mock_resp = mock_base.mock_http_response(resp.to_json())
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            altered = client.alter_policy(
                "policyA",
                PolicyChange.rename("new_policy"),
                PolicyChange.update_comment("new comment"),
            )
            self.check_policy_equal(policy, altered)
            args, kwargs = mock_put.call_args
            self.assertEqual("api/metalakes/metalake_demo/policies/policyA", args[0])
            updates = kwargs["json"].to_dict()["updates"]
            self.assertEqual("rename", updates[0]["@type"])
            self.assertEqual("updateComment", updates[1]["@type"])

    def test_gravitino_alter_policy_api_without_changes(self, *mock_method) -> None:
        client = self._client()
        with self.assertRaises(IllegalArgumentException):
            client.alter_policy("policyA")

    def test_gravitino_delete_policy_api(self, *mock_method) -> None:
        resp = DropResponse(0, True)
        mock_resp = mock_base.mock_http_response(resp.to_json())
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ):
            self.assertTrue(client.delete_policy("policyA"))

    def test_gravitino_list_tag_associations_for_policy(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        tag = mock_base.build_tag_dto()
        association = TagForPolicyAssociationDTO(
            _tag=tag,
            _selector=PolicyAssociationSelectorDTO("ALL_VALUES", None),
        )
        resp = TagForPolicyAssociationListResponse(0, [association])
        policy_resp = PolicyResponse(0, policy)
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=[
                mock_base.mock_http_response(resp.to_json()),
                mock_base.mock_http_response(policy_resp.to_json()),
            ],
        ) as mock_get:
            associations = client.list_tag_associations_for_policy("policyA")
            self.assertEqual(1, len(associations))
            self.assertEqual("policyA", associations[0].policy().name())
            self.assertEqual("tagA", associations[0].tag().name())
            self.assertEqual("ALL_VALUES", associations[0].selector().type())
            first_call = mock_get.call_args_list[0]
            self.assertEqual(
                "api/metalakes/metalake_demo/policies/policyA/tags",
                first_call.args[0],
            )
            self.assertEqual({"details": "true"}, first_call.kwargs["params"])

    def test_gravitino_list_policy_associations_for_tag(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        tag = mock_base.build_tag_dto()
        association = PolicyForTagAssociationDTO(
            _policy=policy,
            _selector=PolicyAssociationSelectorDTO("TAG_VALUE", "gold"),
        )
        resp = PolicyForTagAssociationListResponse(0, [association])
        tag_resp = TagResponse(0, tag)
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=[
                mock_base.mock_http_response(resp.to_json()),
                mock_base.mock_http_response(tag_resp.to_json()),
            ],
        ) as mock_get:
            associations = client.list_policy_associations_for_tag("tagA")
            self.assertEqual(1, len(associations))
            self.assertEqual("policyA", associations[0].policy().name())
            self.assertEqual("tagA", associations[0].tag().name())
            self.assertEqual("gold", associations[0].selector().value())
            first_call = mock_get.call_args_list[0]
            self.assertEqual(
                "api/metalakes/metalake_demo/tags/tagA/policies",
                first_call.args[0],
            )
            self.assertEqual(TAG_ERROR_HANDLER, first_call.kwargs["error_handler"])

    def test_gravitino_add_policy_for_tag(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        tag = mock_base.build_tag_dto()
        resp = PolicyTagAssociationResponse(
            0,
            "policyA",
            "tagA",
            PolicyAssociationSelectorDTO("TAG_VALUE", "gold"),
        )
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_base.mock_http_response(resp.to_json()),
        ) as mock_post, patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=[
                mock_base.mock_http_response(PolicyResponse(0, policy).to_json()),
                mock_base.mock_http_response(TagResponse(0, tag).to_json()),
            ],
        ):
            association = client.add_policy_for_tag(
                "tagA", "policyA", TagValueSelector.of("gold")
            )
            self.assertEqual("policyA", association.policy().name())
            self.assertEqual("tagA", association.tag().name())
            self.assertEqual("gold", association.selector().value())
            args, kwargs = mock_post.call_args
            self.assertEqual(
                "api/metalakes/metalake_demo/tags/tagA/policies/policyA", args[0]
            )
            self.assertEqual(TAG_ERROR_HANDLER, kwargs["error_handler"])
            self.assertEqual("TAG_VALUE", kwargs["json"].selector_dto.type())
            self.assertEqual("gold", kwargs["json"].selector_dto.value())

    def test_gravitino_add_policy_for_tag_default_selector(self, *mock_method) -> None:
        policy = mock_base.build_policy_dto()
        tag = mock_base.build_tag_dto()
        resp = PolicyTagAssociationResponse(
            0,
            "policyA",
            "tagA",
            PolicyAssociationSelectorDTO("ALL_VALUES", None),
        )
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_base.mock_http_response(resp.to_json()),
        ) as mock_post, patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=[
                mock_base.mock_http_response(PolicyResponse(0, policy).to_json()),
                mock_base.mock_http_response(TagResponse(0, tag).to_json()),
            ],
        ):
            association = client.add_policy_for_tag("tagA", "policyA")
            self.assertEqual("ALL_VALUES", association.selector().type())
            _, kwargs = mock_post.call_args
            self.assertEqual("ALL_VALUES", kwargs["json"].selector_dto.type())

    def test_gravitino_remove_policy_from_tag(self, *mock_method) -> None:
        resp = BaseResponse(0)
        client = self._client()

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_base.mock_http_response(resp.to_json()),
        ) as mock_delete:
            self.assertIsNone(client.remove_policy_from_tag("tagA", "policyA"))
            args, kwargs = mock_delete.call_args
            self.assertEqual(
                "api/metalakes/metalake_demo/tags/tagA/policies/policyA", args[0]
            )
            self.assertEqual(TAG_ERROR_HANDLER, kwargs["error_handler"])


if __name__ == "__main__":
    unittest.main()
