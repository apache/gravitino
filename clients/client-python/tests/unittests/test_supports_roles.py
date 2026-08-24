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
from unittest.mock import Mock, patch

from gravitino.api.authorization.supports_roles import SupportsRoles
from gravitino.api.catalog import Catalog
from gravitino.api.file.fileset import Fileset
from gravitino.api.metalake import Metalake
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.model.model import Model
from gravitino.api.rel.table import Table
from gravitino.api.schema import Schema
from gravitino.client.generic_fileset import GenericFileset
from gravitino.client.generic_schema import GenericSchema
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.responses.name_list_response import NameListResponse
from gravitino.exceptions.base import UnsupportedOperationException
from gravitino.exceptions.handlers.role_error_handler import ROLE_ERROR_HANDLER
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient
from tests.unittests import mock_base
from tests.unittests.fixtures.table_fixtures import TABLE_DTO_JSON_STRING


class TestSupportsRoles(unittest.TestCase):
    METALAKE_NAME = "metalake"
    CATALOG_NAME = "catalog"
    SCHEMA_NAME = "schema"
    REST_CLIENT = HTTPClient("http://localhost:8090")

    @classmethod
    def setUpClass(cls) -> None:
        audit = AuditDTO(_creator="test")
        cls.metalake = GravitinoMetalake(
            MetalakeDTO(cls.METALAKE_NAME, "comment", {}, audit), cls.REST_CLIENT
        )
        cls.catalog = RelationalCatalog(
            catalog_namespace=Namespace.of(cls.METALAKE_NAME),
            name=cls.CATALOG_NAME,
            catalog_type=Catalog.Type.RELATIONAL,
            provider="test",
            audit=audit,
            rest_client=cls.REST_CLIENT,
        )
        cls.schema = GenericSchema(
            mock_base.build_schema_dto(name=cls.SCHEMA_NAME),
            cls.REST_CLIENT,
            cls.METALAKE_NAME,
            cls.CATALOG_NAME,
        )
        cls.table = RelationalTable(
            Namespace.of(cls.METALAKE_NAME, cls.CATALOG_NAME, cls.SCHEMA_NAME),
            TableDTO.from_json(TABLE_DTO_JSON_STRING),
            cls.REST_CLIENT,
        )
        cls.fileset = GenericFileset(
            FilesetDTO(
                _name="fileset",
                _comment="comment",
                _type=Fileset.Type.EXTERNAL,
                _properties={},
                _storage_locations={Fileset.LOCATION_NAME_UNKNOWN: "/tmp/fileset"},
                _audit=audit,
            ),
            cls.REST_CLIENT,
            Namespace.of(cls.METALAKE_NAME, cls.CATALOG_NAME, cls.SCHEMA_NAME),
        )

    def test_list_roles_for_metalake(self) -> None:
        self._test_list_roles(
            self.metalake.supports_roles(),
            MetadataObjects.of([self.METALAKE_NAME], MetadataObject.Type.METALAKE),
        )

    def test_list_roles_for_catalog(self) -> None:
        self._test_list_roles(
            self.catalog.supports_roles(),
            MetadataObjects.of([self.CATALOG_NAME], MetadataObject.Type.CATALOG),
        )

    def test_list_roles_for_schema(self) -> None:
        self._test_list_roles(
            self.schema.supports_roles(),
            MetadataObjects.of(
                [self.CATALOG_NAME, self.SCHEMA_NAME], MetadataObject.Type.SCHEMA
            ),
        )

    def test_list_roles_for_table(self) -> None:
        self._test_list_roles(
            self.table.supports_roles(),
            MetadataObjects.of(
                [self.CATALOG_NAME, self.SCHEMA_NAME, self.table.name()],
                MetadataObject.Type.TABLE,
            ),
        )

    def test_list_roles_for_fileset(self) -> None:
        self._test_list_roles(
            self.fileset.supports_roles(),
            MetadataObjects.of(
                [self.CATALOG_NAME, self.SCHEMA_NAME, self.fileset.name()],
                MetadataObject.Type.FILESET,
            ),
        )

    def test_default_supports_roles_raises_unsupported_operation(self) -> None:
        metadata_object_types = [Metalake, Catalog, Schema, Table, Fileset, Model]

        for metadata_object_type in metadata_object_types:
            with self.subTest(metadata_object_type=metadata_object_type.__name__):
                metadata_object = Mock(spec=metadata_object_type)
                with self.assertRaises(UnsupportedOperationException):
                    metadata_object_type.supports_roles(metadata_object)

    def _test_list_roles(
        self, supports_roles: SupportsRoles, metadata_object: MetadataObject
    ) -> None:
        expected_roles = ["role1", "role2"]
        mock_response = mock_base.mock_http_response(
            NameListResponse(0, expected_roles).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_response,
        ) as mock_get:
            self.assertEqual(expected_roles, supports_roles.list_binding_role_names())
            mock_get.assert_called_once_with(
                "api/metalakes/metalake/objects/"
                f"{metadata_object.type().name.lower()}/"
                f"{metadata_object.full_name()}/roles",
                params={},
                error_handler=ROLE_ERROR_HANDLER,
            )
