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
from typing import Optional
from unittest.mock import Mock, patch

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.credential.supports_credentials import SupportsCredentials
from gravitino.api.rel.column import Column
from gravitino.api.rel.dialects import Dialects
from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.table_change import TableChange
from gravitino.api.rel.types.types import Types
from gravitino.api.rel.view_change import ViewChange
from gravitino.api.secret.supports_secrets import SupportsSecrets
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.rel.view_dto import ViewDTO
from gravitino.dto.requests.view_update_request import (
    ViewUpdateRequest,
    ViewUpdateRequestBase,
)
from gravitino.dto.requests.view_updates_request import ViewUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.table_response import TableResponse
from gravitino.dto.responses.view_response import ViewResponse
from gravitino.dto.util.dto_converters import DTOConverters
from gravitino.exceptions.base import (
    IllegalArgumentException,
    IllegalNameIdentifierException,
    IllegalNamespaceException,
    NoSuchSchemaException,
    NoSuchTableException,
    NoSuchViewException,
    TableAlreadyExistsException,
    ViewAlreadyExistsException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient, Response
from tests.unittests.fixtures.table_fixtures import (
    TABLE_DTO_JSON_STRING_WITH_STARTING_DATE_SORT,
)


class TestRelationalCatalog(unittest.TestCase):
    @classmethod
    def _default_columns(cls) -> list[ColumnDTO]:
        return [
            ColumnDTO(
                _name="id",
                _data_type=Types.IntegerType.get(),
                _comment="id column",
                _nullable=False,
            )
        ]

    @classmethod
    def _default_representations(cls) -> list[SQLRepresentationDTO]:
        return [
            SQLRepresentationDTO(
                _dialect=Dialects.TRINO,
                _sql="SELECT id FROM test_table",
            )
        ]

    @classmethod
    def _default_api_columns(cls) -> list[Column]:
        return [
            Column.of(
                "id",
                Types.IntegerType.get(),
                comment="id column",
                nullable=False,
            )
        ]

    @classmethod
    def _default_api_representations(cls) -> list[Representation]:
        return [SQLRepresentation(Dialects.TRINO, "SELECT id FROM test_table")]

    @classmethod
    def _new_view_dto(
        cls,
        name: str,
        comment: str = "test view comment",
        columns: Optional[list[ColumnDTO]] = None,
        representations: Optional[list[SQLRepresentationDTO]] = None,
        properties: Optional[dict[str, str]] = None,
        default_catalog: Optional[str] = "test_catalog",
        default_schema: Optional[str] = "test_schema",
    ) -> ViewDTO:
        return ViewDTO(
            _name=name,
            _columns=columns if columns is not None else cls._default_columns(),
            _representations=(
                representations
                if representations is not None
                else cls._default_representations()
            ),
            _comment=comment,
            _default_catalog=default_catalog,
            _default_schema=default_schema,
            _properties=properties if properties is not None else {},
            _audit=AuditDTO(
                "creator", "2022-01-01T00:00:00Z", "modifier", "2022-01-01T00:00:00Z"
            ),
        )

    @classmethod
    def setUpClass(cls) -> None:
        cls.metalake_name = "test_metalake"
        cls.catalog_name = "test_catalog"
        cls.schema_name = "test_schema"
        cls.table_name = "test_table"
        cls.catalog_namespace = Namespace.of(cls.metalake_name)
        cls.table_identifier = NameIdentifier.of(cls.schema_name, cls.table_name)
        cls.view_name = "test_view"
        cls.view_identifier = NameIdentifier.of(cls.schema_name, cls.view_name)
        cls.rest_client = HTTPClient("http://localhost:8090")
        cls.catalog = RelationalCatalog(
            catalog_namespace=cls.catalog_namespace,
            name=cls.catalog_name,
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=cls.rest_client,
        )
        cls.TABLE_DTO_JSON_STRING = TABLE_DTO_JSON_STRING_WITH_STARTING_DATE_SORT
        cls.table_dto = TableDTO.from_json(cls.TABLE_DTO_JSON_STRING)
        cls.view_dto = cls._new_view_dto(cls.view_name, properties={"k1": "v1"})

    def _get_mock_http_resp(self, json_str: str, return_code: int = 200):
        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = return_code
        mock_http_resp.read.return_value = json_str
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)
        return mock_resp

    def test_as_table_catalog(self):
        table_catalog = self.catalog.as_table_catalog()
        self.assertIs(table_catalog, self.catalog)

    def test_as_view_catalog(self):
        view_catalog = self.catalog.as_view_catalog()
        self.assertIs(view_catalog, self.catalog)

    def test_supports_credentials(self):
        self.assertIsInstance(self.catalog, SupportsCredentials)
        self.assertIs(self.catalog.supports_credentials(), self.catalog)
        self.assertIs(self.catalog.support_credentials(), self.catalog)

    def test_supports_secrets(self):
        self.assertIsInstance(self.catalog, SupportsSecrets)
        self.assertIs(self.catalog.supports_secrets(), self.catalog)
        self.assertIs(self.catalog.support_secrets(), self.catalog)

    def test_create_table(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ):
            table = self.catalog.create_table(
                identifier=self.table_identifier,
                columns=DTOConverters.from_dtos(self.table_dto.columns()),
                partitioning=DTOConverters.from_dtos(self.table_dto.partitioning()),
                distribution=DTOConverters.from_dto(
                    self.table_dto.distribution() or DistributionDTO.NONE
                ),
                sort_orders=DTOConverters.from_dtos(self.table_dto.sort_order()),
                indexes=DTOConverters.from_dtos(self.table_dto.index()),
                properties=self.table_dto.properties(),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_load_table(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            table = self.catalog.load_table(self.table_identifier)
            self.assertEqual(table.name(), self.table_dto.name())

    def test_load_table_with_vector_similarity_index(self):
        response_json = json.loads(TableResponse(0, self.table_dto).to_json())
        response_json["table"]["indexes"] = [
            {
                "indexType": "DATA_SKIPPING_VECTOR_SIMILARITY",
                "name": "idx_vector",
                "fieldNames": [["id"]],
                "properties": {
                    "type": "hnsw",
                    "distance_function": "L2Distance",
                    "dimensions": "3",
                },
            }
        ]
        mock_resp = self._get_mock_http_resp(json.dumps(response_json))

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            table = self.catalog.load_table(self.table_identifier)

        self.assertEqual(table.name(), self.table_dto.name())
        self.assertEqual(len(table.index()), 1)
        self.assertEqual(
            table.index()[0].type(), Index.IndexType.DATA_SKIPPING_VECTOR_SIMILARITY
        )
        self.assertEqual(
            table.index()[0].properties(),
            {"type": "hnsw", "distance_function": "L2Distance", "dimensions": "3"},
        )

    def test_load_table_with_required_privilege_names(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            privileges = {Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE}
            table = self.catalog.load_table(
                self.table_identifier, required_privilege_names=privileges
            )
            self.assertEqual(table.name(), self.table_dto.name())
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            self.assertEqual(
                call_args.kwargs["params"]["privileges"], "MODIFY_TABLE,SELECT_TABLE"
            )

    def test_list_tables(self):
        resp_body = EntityListResponse(
            0,
            [
                NameIdentifier.of(
                    self.metalake_name,
                    self.catalog_name,
                    self.schema_name,
                    self.table_name,
                )
            ],
        )
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            tables = self.catalog.list_tables(namespace=Namespace.of(self.schema_name))
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0], self.table_identifier)

    def test_load_table_not_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchTableException("Table not found"),
        ):
            with self.assertRaises(NoSuchTableException):
                self.catalog.load_table(self.table_identifier)

    def test_table_exists_with_missing_schema_or_table(self):
        for failure in (
            NoSuchSchemaException("Schema not found"),
            NoSuchTableException("Table not found"),
        ):
            with self.subTest(failure=type(failure).__name__):
                with patch(
                    "gravitino.utils.http_client.HTTPClient.get",
                    side_effect=failure,
                ):
                    with self.assertRaises(type(failure)):
                        self.catalog.load_table(self.table_identifier)
                    self.assertFalse(self.catalog.table_exists(self.table_identifier))
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=RuntimeError("Backend unavailable"),
        ):
            with self.assertRaises(RuntimeError):
                self.catalog.table_exists(self.table_identifier)

    def test_create_table_already_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=TableAlreadyExistsException("Table already exists"),
        ):
            with self.assertRaises(TableAlreadyExistsException):
                self.catalog.create_table(
                    identifier=self.table_identifier,
                    columns=DTOConverters.from_dtos(self.table_dto.columns()),
                )

    def test_list_tables_invalid_namespace(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSchemaException("Schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.list_tables(namespace=Namespace.of("invalid_schema"))

    def test_drop_table(self):
        resp_body = DropResponse(0, True)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ) as mock_delete:
            is_dropped = self.catalog.drop_table(self.table_identifier)
            self.assertTrue(is_dropped)
            mock_delete.assert_called_once()
            call_args = mock_delete.call_args
            self.assertIsNone(call_args.kwargs.get("params"))

    def test_purge_table(self):
        resp_body = DropResponse(0, True)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ) as mock_delete:
            is_dropped = self.catalog.purge_table(self.table_identifier)
            self.assertTrue(is_dropped)
            mock_delete.assert_called_once()
            call_args = mock_delete.call_args
            self.assertIn("params", call_args.kwargs)
            self.assertEqual(call_args.kwargs["params"], {"purge": "true"})

    def test_alter_table(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.update_comment("Updated comment"),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_not_exists(self):
        with (
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                side_effect=NoSuchTableException("Table not found"),
            ),
            self.assertRaises(NoSuchTableException),
        ):
            self.catalog.alter_table(
                self.table_identifier,
                TableChange.update_comment("Updated comment"),
            )

    def test_alter_table_set_property(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.set_property("key", "value"),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_rename(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.rename("new_table_name"),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_remove_property(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.remove_property("key"),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_rename_column(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.rename_column(["id"], "new_id"),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_update_column_comment(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.update_column_comment(["id"], "new comment"),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_delete_column(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.delete_column(["id"], if_exists=True),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_alter_table_update_column_nullability(self):
        resp_body = TableResponse(0, self.table_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ):
            table = self.catalog.alter_table(
                self.table_identifier,
                TableChange.update_column_nullability(["id"], nullable=True),
            )
            self.assertEqual(table.name(), self.table_dto.name())

    def test_list_views(self):
        view1 = NameIdentifier.of(
            self.metalake_name, self.catalog_name, self.schema_name, "view1"
        )
        view2 = NameIdentifier.of(
            self.metalake_name, self.catalog_name, self.schema_name, "view2"
        )

        resp_body = EntityListResponse(0, [view1, view2])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            views = self.catalog.as_view_catalog().list_views(
                Namespace.of(self.schema_name)
            )
            self.assertEqual(2, len(views))
            self.assertEqual("view1", views[0].name())
            self.assertEqual("view2", views[1].name())

    def test_list_views_invalid_namespace(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSchemaException("Schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.as_view_catalog().list_views(
                    Namespace.of("invalid_schema")
                )

    def test_list_views_empty(self):
        resp_body = EntityListResponse(0, [])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            views = self.catalog.as_view_catalog().list_views(
                Namespace.of(self.schema_name)
            )
            self.assertEqual(0, len(views))

    def test_list_views_server_internal_error(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=RuntimeError("internal error"),
        ):
            with self.assertRaises(RuntimeError):
                self.catalog.as_view_catalog().list_views(
                    Namespace.of(self.schema_name)
                )

    def test_list_views_invalid_namespace_length(self):
        with self.assertRaisesRegex(
            IllegalNamespaceException,
            "View namespace must be non-null and have 1 level",
        ):
            self.catalog.as_view_catalog().list_views(Namespace.of("s1", "s2"))

        with self.assertRaisesRegex(
            IllegalNamespaceException,
            "View namespace must be non-null and have 1 level",
        ):
            self.catalog.as_view_catalog().list_views(Namespace.empty())

    def test_load_view(self):
        resp_body = ViewResponse(0, self.view_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            view = self.catalog.as_view_catalog().load_view(self.view_identifier)
            self.assertEqual(self.view_name, view.name())
            self.assertEqual("test view comment", view.comment())
            self.assertEqual("test_catalog", view.default_catalog())
            self.assertEqual("test_schema", view.default_schema())
            self.assertEqual("v1", view.properties()["k1"])
            self.assertEqual(
                "SELECT id FROM test_table", view.sql_for(Dialects.TRINO).sql()
            )

    def test_load_view_not_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchViewException("View not found"),
        ):
            with self.assertRaises(NoSuchViewException):
                self.catalog.as_view_catalog().load_view(self.view_identifier)

    def test_load_view_invalid_identifier(self):
        with self.assertRaisesRegex(
            IllegalNameIdentifierException, "NameIdentifier must not be null"
        ):
            self.catalog.as_view_catalog().load_view(None)

        with self.assertRaisesRegex(
            IllegalNamespaceException,
            "View namespace must be non-null and have 1 level",
        ):
            self.catalog.as_view_catalog().load_view(
                NameIdentifier.of("s1", "s2", "view1")
            )

    def test_create_view(self):
        resp_body = ViewResponse(0, self.view_dto)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ):
            view = self.catalog.as_view_catalog().create_view(
                self.view_identifier,
                self._default_api_columns(),
                self._default_api_representations(),
                comment="test view comment",
                default_catalog="test_catalog",
                default_schema="test_schema",
                properties={"k1": "v1"},
            )
            self.assertEqual(self.view_name, view.name())
            self.assertEqual("test view comment", view.comment())

    def test_create_view_already_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=ViewAlreadyExistsException("View already exists"),
        ):
            with self.assertRaises(ViewAlreadyExistsException):
                self.catalog.as_view_catalog().create_view(
                    self.view_identifier,
                    self._default_api_columns(),
                    self._default_api_representations(),
                )

    def test_create_view_schema_not_found(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=NoSuchSchemaException("Schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.as_view_catalog().create_view(
                    self.view_identifier,
                    self._default_api_columns(),
                    self._default_api_representations(),
                )

    def test_create_view_rejects_empty_representations(self):
        with self.assertRaisesRegex(
            IllegalArgumentException,
            '"representations" field is required and cannot be empty',
        ):
            self.catalog.as_view_catalog().create_view(
                self.view_identifier,
                self._default_api_columns(),
                [],
            )

        with self.assertRaisesRegex(
            IllegalArgumentException,
            '"representations" field is required and cannot be empty',
        ):
            self.catalog.as_view_catalog().create_view(
                self.view_identifier,
                self._default_api_columns(),
                None,
            )

    def test_create_view_invalid_identifier(self):
        with self.assertRaises(IllegalNamespaceException):
            self.catalog.as_view_catalog().create_view(
                NameIdentifier.of("s1", "s2", "v1"),
                self._default_api_columns(),
                self._default_api_representations(),
            )

    def _run_alter_view(
        self,
        ident: NameIdentifier,
        req: ViewUpdateRequestBase,
        updated: ViewDTO,
        change: ViewChange,
    ):
        full_namespace = Namespace.of(
            self.metalake_name, self.catalog_name, ident.namespace().level(0)
        )
        # pylint: disable=protected-access
        view_path = (
            f"{self.catalog._format_view_request_path(full_namespace)}/{ident.name()}"
        )
        updates_request = ViewUpdatesRequest(_updates=[req])
        resp_body = ViewResponse(0, updated)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            altered = self.catalog.as_view_catalog().alter_view(ident, change)
            self.assertEqual(mock_put.call_args[0][0], view_path)
            self.assertEqual(mock_put.call_args[1]["json"], updates_request)
            self.assertEqual(updated.name(), altered.name())
            self.assertEqual(updated.comment(), altered.comment())
            self.assertEqual(updated.properties(), altered.properties())
            self.assertEqual(
                len(updated.representations()), len(altered.representations())
            )
            return altered

    def test_alter_view_rename(self):
        ident = NameIdentifier.of(self.schema_name, "view1")
        updated = self._new_view_dto(
            "view2",
            "comment",
            self._default_columns(),
            self._default_representations(),
        )
        req = ViewUpdateRequest.RenameViewRequest(_new_name="view2")
        self._run_alter_view(ident, req, updated, ViewChange.rename("view2"))

    test_alter_view = test_alter_view_rename

    def test_alter_view_not_exists(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            side_effect=NoSuchViewException("View not found"),
        ):
            with self.assertRaises(NoSuchViewException):
                self.catalog.as_view_catalog().alter_view(
                    self.view_identifier,
                    ViewChange.rename("new_view"),
                )

    def test_alter_view_set_property(self):
        ident = NameIdentifier.of(self.schema_name, "view1")
        updated = self._new_view_dto(
            "view1",
            "comment",
            self._default_columns(),
            self._default_representations(),
            properties={"k1": "v1"},
        )
        req = ViewUpdateRequest.SetViewPropertyRequest(_property="k1", _value="v1")
        self._run_alter_view(ident, req, updated, ViewChange.set_property("k1", "v1"))

    def test_alter_view_remove_property(self):
        ident = NameIdentifier.of(self.schema_name, "view1")
        updated = self._new_view_dto(
            "view1",
            "comment",
            self._default_columns(),
            self._default_representations(),
        )
        req = ViewUpdateRequest.RemoveViewPropertyRequest(_property="k1")
        self._run_alter_view(ident, req, updated, ViewChange.remove_property("k1"))

    def test_alter_view_replace(self):
        ident = NameIdentifier.of(self.schema_name, "view1")
        new_rep = SQLRepresentationDTO(
            _dialect=Dialects.TRINO,
            _sql="SELECT 3",
        )
        new_columns = self._default_columns()
        new_reps = [new_rep]
        updated = self._new_view_dto(
            "view1",
            "new comment",
            new_columns,
            new_reps,
            default_catalog=None,
            default_schema=None,
        )

        req = ViewUpdateRequest.ReplaceViewRequest(
            _columns=new_columns,
            _representations=new_reps,
            _default_catalog=None,
            _default_schema=None,
            _comment="new comment",
        )
        new_rep_api = SQLRepresentation(Dialects.TRINO, "SELECT 3")
        altered = self._run_alter_view(
            ident,
            req,
            updated,
            ViewChange.replace_view(
                self._default_api_columns(),
                [new_rep_api],
                None,
                None,
                "new comment",
            ),
        )
        self.assertIsNone(altered.default_catalog())
        self.assertIsNone(altered.default_schema())

    def test_alter_view_invalid_identifier(self):
        with self.assertRaises(IllegalNamespaceException):
            self.catalog.as_view_catalog().alter_view(
                NameIdentifier.of("s1", "s2", "v1"),
                ViewChange.rename("new_name"),
            )

    def test_drop_view(self):
        resp_body = DropResponse(0, True)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ):
            is_dropped = self.catalog.as_view_catalog().drop_view(self.view_identifier)
            self.assertTrue(is_dropped)

    def test_drop_view_not_exists(self):
        resp_body = DropResponse(0, False)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ):
            is_dropped = self.catalog.as_view_catalog().drop_view(self.view_identifier)
            self.assertFalse(is_dropped)

    def test_drop_view_server_internal_error(self):
        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            side_effect=RuntimeError("internal error"),
        ):
            with self.assertRaises(RuntimeError):
                self.catalog.as_view_catalog().drop_view(self.view_identifier)

    def test_drop_view_invalid_identifier(self):
        with self.assertRaises(IllegalNamespaceException):
            self.catalog.as_view_catalog().drop_view(
                NameIdentifier.of("s1", "s2", "v1")
            )

    def test_format_table_request_path_invalid_namespace(self):
        with self.assertRaisesRegex(
            IllegalNamespaceException,
            "Table full namespace must be non-null and have 3 levels in the format",
        ):
            self.catalog._format_table_request_path(  # pylint: disable=protected-access
                Namespace.of("schema1")
            )

    def test_format_view_request_path_invalid_namespace(self):
        with self.assertRaisesRegex(
            IllegalNamespaceException,
            "View full namespace must be non-null and have 3 levels in the format",
        ):
            self.catalog._format_view_request_path(  # pylint: disable=protected-access
                Namespace.of("schema1")
            )

    def test_to_view_update_request(self):
        to_request = (
            RelationalCatalog._to_view_update_request  # pylint: disable=protected-access
        )

        rename_request = to_request(ViewChange.rename("new_view"))
        set_property_request = to_request(ViewChange.set_property("key", "value"))
        remove_property_request = to_request(ViewChange.remove_property("key"))
        replace_view_request = to_request(
            ViewChange.replace_view(
                [Column.of("id", Types.IntegerType.get())],
                [SQLRepresentation(Dialects.TRINO, "SELECT id FROM table")],
                "catalog",
                "schema",
                "comment",
            )
        )

        self.assertIsInstance(rename_request, ViewUpdateRequest.RenameViewRequest)
        self.assertEqual("new_view", rename_request.view_change().new_name())
        self.assertIsInstance(
            set_property_request, ViewUpdateRequest.SetViewPropertyRequest
        )
        self.assertEqual("key", set_property_request.view_change().property())
        self.assertEqual("value", set_property_request.view_change().value())
        self.assertIsInstance(
            remove_property_request, ViewUpdateRequest.RemoveViewPropertyRequest
        )
        self.assertEqual("key", remove_property_request.view_change().property())
        self.assertIsInstance(
            replace_view_request, ViewUpdateRequest.ReplaceViewRequest
        )
        self.assertEqual(
            "catalog", replace_view_request.view_change().default_catalog()
        )
        self.assertEqual("schema", replace_view_request.view_change().default_schema())
        self.assertEqual("comment", replace_view_request.view_change().comment())

    def test_to_view_update_request_unsupported_change(self):
        class UnsupportedViewChange(ViewChange):
            pass

        with self.assertRaisesRegex(IllegalArgumentException, "Unknown change type"):
            RelationalCatalog._to_view_update_request(  # pylint: disable=protected-access
                UnsupportedViewChange()
            )
