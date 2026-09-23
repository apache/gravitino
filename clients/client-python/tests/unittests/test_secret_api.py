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
from http.client import HTTPResponse
from unittest.mock import Mock, patch

from gravitino.api.rel.dialects import Dialects
from gravitino.client.generic_model import GenericModel
from gravitino.client.generic_view import GenericView
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.model_dto import ModelDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.rel.view_dto import ViewDTO
from gravitino.namespace import Namespace
from gravitino.utils import Response, HTTPClient
from tests.unittests.fixtures.table_fixtures import TABLE_DTO_JSON_STRING


class TestSecretApi(unittest.TestCase):
    """Behavioral tests for SupportsSecrets wiring on metalake / table / view / model."""

    METALAKE = "metalake_secrets"
    CATALOG = "catalog1"
    SCHEMA = "schema1"

    def setUp(self):
        self.rest_client = HTTPClient("http://localhost:8090")
        self.ns = Namespace.of(self.METALAKE, self.CATALOG, self.SCHEMA)
        self.expected_secrets = {"jdbc-password": "s3cr3t", "custom-token": "t"}

    def _mock_resp(self, secrets: dict):
        body = json.dumps({"code": 0, "secrets": secrets})
        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = 200
        mock_http_resp.read.return_value = body
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        return Response(mock_http_resp)

    def _assert_get_secrets(self, supports_secrets, expected_path: str):
        # pylint: disable=protected-access
        self.assertEqual(
            expected_path, supports_secrets._object_secret_operations._request_path
        )
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=self._mock_resp(self.expected_secrets),
        ) as mock_get:
            secrets = supports_secrets.support_secrets().get_secrets()
            self.assertEqual(self.expected_secrets, secrets)
            mock_get.assert_called()
            self.assertEqual(expected_path, mock_get.call_args.args[0])

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=self._mock_resp({}),
        ):
            self.assertEqual({}, supports_secrets.get_secrets())

    def test_get_secrets_for_metalake(self):
        metalake = GravitinoMetalake(
            MetalakeDTO(
                _name=self.METALAKE,
                _comment="c",
                _properties={},
                _audit=AuditDTO("test"),
            ),
            self.rest_client,
        )
        self._assert_get_secrets(
            metalake,
            f"api/metalakes/{self.METALAKE}/objects/metalake/{self.METALAKE}/secrets",
        )

    def test_get_secrets_for_table(self):
        table_dto = TableDTO.from_json(TABLE_DTO_JSON_STRING)
        table = RelationalTable(self.ns, table_dto, self.rest_client)
        full_name = f"{self.CATALOG}.{self.SCHEMA}.{table.name()}"
        self.assertEqual("example_table", table.name())
        self._assert_get_secrets(
            table,
            f"api/metalakes/{self.METALAKE}/objects/table/{full_name}/secrets",
        )

    def test_get_secrets_for_view(self):
        view = GenericView(
            ViewDTO(
                _name="view1",
                _representations=[
                    SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql="SELECT 1")
                ],
                _comment="c",
                _properties={},
                _audit=AuditDTO("test"),
            ),
            self.rest_client,
            self.ns,
        )
        full_name = f"{self.CATALOG}.{self.SCHEMA}.view1"
        self._assert_get_secrets(
            view,
            f"api/metalakes/{self.METALAKE}/objects/view/{full_name}/secrets",
        )

    def test_get_secrets_for_model(self):
        model = GenericModel(
            ModelDTO(
                _name="model1",
                _comment="c",
                _properties={},
                _latest_version=0,
                _audit=AuditDTO("test"),
            ),
            self.rest_client,
            self.ns,
        )
        full_name = f"{self.CATALOG}.{self.SCHEMA}.model1"
        self._assert_get_secrets(
            model,
            f"api/metalakes/{self.METALAKE}/objects/model/{full_name}/secrets",
        )


if __name__ == "__main__":
    unittest.main()
