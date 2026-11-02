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

import unittest
from unittest.mock import patch

from gravitino.api.metadata_objects import MetadataObject, MetadataObjects
from gravitino.api.policy.policy import Policy
from gravitino.client.generic_policy import GenericPolicy
from gravitino.client.metadata_object_policy_operations import (
    MetadataObjectPolicyOperations,
)
from gravitino.dto.responses.policy_response import (
    MetadataObjectPolicyListResponse,
    PolicyNameListResponse,
)
from gravitino.exceptions.handlers.policy_error_handler import POLICY_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient
from tests.unittests import mock_base


class TestMetadataObjectPolicyOperations(unittest.TestCase):
    REST_CLIENT = HTTPClient("http://localhost:8090")
    METALAKE_NAME = "demo_metalake"

    def test_list_policy_request_path(self) -> None:
        metadata_object = MetadataObjects.of(
            ["catalog", "schema", "table"], MetadataObject.Type.TABLE
        )
        table_policy_request_path = (
            MetadataObjectPolicyOperations.POLICY_REQUEST_PATH.format(
                encode_string(TestMetadataObjectPolicyOperations.METALAKE_NAME),
                metadata_object.type().name.lower(),
                encode_string(metadata_object.full_name()),
            )
        )
        self.assertEqual(
            "api/metalakes/demo_metalake/objects/table/catalog.schema.table/policies",
            table_policy_request_path,
        )

    def test_list_policies(self) -> None:
        policy_operations = MetadataObjectPolicyOperations(
            TestMetadataObjectPolicyOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"],
                MetadataObject.Type.TABLE,
            ),
            TestMetadataObjectPolicyOperations.REST_CLIENT,
        )
        json_str = PolicyNameListResponse(0, ["policyA", "policyB"], True).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            policies = policy_operations.list_policies()

            self.assertEqual(["policyA", "policyB"], policies)
            mock_get.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/policies",
                params={},
                error_handler=POLICY_ERROR_HANDLER,
            )

    def test_list_policies_with_visibility(self) -> None:
        policy_operations = MetadataObjectPolicyOperations(
            TestMetadataObjectPolicyOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema"],
                MetadataObject.Type.SCHEMA,
            ),
            TestMetadataObjectPolicyOperations.REST_CLIENT,
        )
        json_str = PolicyNameListResponse(0, ["policyA"], True).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            policy_list = policy_operations.list_policies_with_visibility()

            self.assertEqual(["policyA"], policy_list.policies())
            self.assertTrue(policy_list.has_inaccessible_policies())

    def test_list_policy_infos(self) -> None:
        policy_operations = MetadataObjectPolicyOperations(
            TestMetadataObjectPolicyOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"],
                MetadataObject.Type.TABLE,
            ),
            TestMetadataObjectPolicyOperations.REST_CLIENT,
        )
        policy = mock_base.build_policy_dto()
        json_str = MetadataObjectPolicyListResponse(0, [policy], False).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            policies = policy_operations.list_policy_infos()

            self.assertEqual(1, len(policies))
            self.assertIsInstance(policies[0], GenericPolicy)
            self.assertIsInstance(policies[0], Policy)
            self.assertEqual(policy.name(), policies[0].name())
            self.assertEqual(policy.comment(), policies[0].comment())
            mock_get.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/policies",
                params={"details": "true"},
                error_handler=POLICY_ERROR_HANDLER,
            )

    def test_list_policy_infos_with_visibility(self) -> None:
        policy_operations = MetadataObjectPolicyOperations(
            TestMetadataObjectPolicyOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog"],
                MetadataObject.Type.CATALOG,
            ),
            TestMetadataObjectPolicyOperations.REST_CLIENT,
        )
        policy = mock_base.build_policy_dto()
        json_str = MetadataObjectPolicyListResponse(0, [policy], True).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            policy_list = policy_operations.list_policy_infos_with_visibility()

            self.assertEqual(1, len(policy_list.policies()))
            self.assertTrue(policy_list.has_inaccessible_policies())


if __name__ == "__main__":
    unittest.main()
