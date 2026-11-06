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
from gravitino.api.policy.supports_policies import SupportsPolicies
from gravitino.client.generic_policy import GenericPolicy
from gravitino.client.generic_schema import GenericSchema
from gravitino.client.metadata_object_policy_operations import (
    MetadataObjectPolicyOperations,
)
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.responses.policy_response import (
    MetadataObjectPolicyListResponse,
    PolicyNameListResponse,
)
from gravitino.dto.responses.schema_response import SchemaResponse
from gravitino.dto.responses.table_response import TableResponse
from gravitino.exceptions.handlers.policy_error_handler import POLICY_ERROR_HANDLER
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient
from tests.unittests import mock_base
from tests.unittests.fixtures.table_fixtures import (
    TABLE_DTO_JSON_STRING_WITH_STARTING_DATE_SORT,
)


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


class TestPoliciesThroughLoadedObjects(unittest.TestCase):
    """Policy listing through loaded metadata objects.

    The objects returned by the catalog load methods must expose the policy
    operations themselves; these tests never instantiate
    MetadataObjectPolicyOperations directly.
    """

    def test_list_policies_through_loaded_schema(self) -> None:
        catalog = mock_base.mock_load_catalog("fileset_catalog")
        schema_response = SchemaResponse(0, mock_base.build_schema_dto())
        policies_response = PolicyNameListResponse(0, ["policyA", "policyB"], True)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=[
                mock_base.mock_http_response(schema_response.to_json()),
                mock_base.mock_http_response(policies_response.to_json()),
                mock_base.mock_http_response(policies_response.to_json()),
            ],
        ) as mock_get:
            schema = catalog.as_schemas().load_schema("demo_schema")

            self.assertIsInstance(schema, GenericSchema)
            self.assertIsInstance(schema, SupportsPolicies)
            self.assertIs(schema, schema.supports_policies())
            self.assertEqual(
                ["policyA", "policyB"], schema.supports_policies().list_policies()
            )
            policy_list = schema.list_policies_with_visibility()
            self.assertEqual(["policyA", "policyB"], policy_list.policies())
            self.assertTrue(policy_list.has_inaccessible_policies())
            self.assertEqual(
                "api/metalakes/metalake_demo/objects/schema"
                "/fileset_catalog.demo_schema/policies",
                mock_get.call_args_list[1].args[0],
            )

    def test_list_policies_through_loaded_table(self) -> None:
        rest_client = HTTPClient("http://localhost:8090")
        catalog = RelationalCatalog(
            catalog_namespace=Namespace.of("test_metalake"),
            name="test_catalog",
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=rest_client,
        )
        table_dto = TableDTO.from_json(TABLE_DTO_JSON_STRING_WITH_STARTING_DATE_SORT)
        table_response = TableResponse(0, table_dto)
        policies_response = PolicyNameListResponse(0, ["policyA"], False)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=[
                mock_base.mock_http_response(table_response.to_json()),
                mock_base.mock_http_response(policies_response.to_json()),
            ],
        ) as mock_get:
            table = catalog.load_table(
                NameIdentifier.of("test_schema", table_dto.name())
            )

            self.assertIsInstance(table, RelationalTable)
            self.assertIsInstance(table, SupportsPolicies)
            self.assertIs(table, table.supports_policies())
            self.assertEqual(["policyA"], table.supports_policies().list_policies())
            self.assertEqual(
                "api/metalakes/test_metalake/objects/table"
                f"/test_catalog.test_schema.{table_dto.name()}/policies",
                mock_get.call_args_list[1].args[0],
            )

    def test_list_policies_through_loaded_catalog(self) -> None:
        rest_client = HTTPClient("http://localhost:8090")
        catalog = RelationalCatalog(
            catalog_namespace=Namespace.of("test_metalake"),
            name="test_catalog",
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=rest_client,
        )
        policies_response = PolicyNameListResponse(0, ["policyA"], False)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_base.mock_http_response(policies_response.to_json()),
        ) as mock_get:
            self.assertIsInstance(catalog, SupportsPolicies)
            self.assertIs(catalog, catalog.supports_policies())
            self.assertEqual(["policyA"], catalog.supports_policies().list_policies())
            self.assertEqual(
                "api/metalakes/test_metalake/objects/catalog/test_catalog/policies",
                mock_get.call_args_list[0].args[0],
            )


if __name__ == "__main__":
    unittest.main()
