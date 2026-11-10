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
from unittest.mock import patch, Mock

from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.exceptions.base import (
    IllegalArgumentException,
    NoSuchSchemaException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient, Response


class TestBaseSchemaCatalog(unittest.TestCase):
    metalake_name = "test_metalake"
    catalog_name = "test_catalog"
    catalog_namespace = Namespace.of(metalake_name)

    @classmethod
    def setUpClass(cls) -> None:
        cls.rest_client = HTTPClient("http://localhost:8090")
        cls.catalog = RelationalCatalog(
            catalog_namespace=cls.catalog_namespace,
            name=cls.catalog_name,
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=cls.rest_client,
        )

    def _get_mock_http_resp(self, json_str: str, return_code: int = 200):
        mock_http_resp = Mock()
        mock_http_resp.getcode.return_value = return_code
        mock_http_resp.read.return_value = json_str.encode("utf-8")
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        return Response(mock_http_resp)

    def test_list_schemas(self):
        schema_a = NameIdentifier.of(self.metalake_name, self.catalog_name, "a")
        schema_b = NameIdentifier.of(self.metalake_name, self.catalog_name, "b")

        resp_body = EntityListResponse(_code=0, _idents=[schema_a, schema_b])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            schemas = self.catalog.as_schemas().list_schemas()
            self.assertEqual(["a", "b"], schemas)
            # No parentSchema query param when listing all schemas.
            self.assertIsNone(mock_get.call_args.kwargs["params"])

    def test_list_schemas_under_parent(self):
        schema_abc = NameIdentifier.of(self.metalake_name, self.catalog_name, "a:b:c")

        resp_body = EntityListResponse(_code=0, _idents=[schema_abc])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            schemas = self.catalog.as_schemas().list_schemas("a:b")
            self.assertEqual(["a:b:c"], schemas)
            # The parent schema is passed through as a query param.
            self.assertEqual(
                {"parentSchema": "a:b"}, mock_get.call_args.kwargs["params"]
            )

    def test_list_schemas_with_blank_parent(self):
        for blank in ["", "   "]:
            with self.assertRaises(IllegalArgumentException):
                self.catalog.as_schemas().list_schemas(blank)

    def test_list_schemas_under_missing_parent(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSchemaException("schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.as_schemas().list_schemas("a:b")
