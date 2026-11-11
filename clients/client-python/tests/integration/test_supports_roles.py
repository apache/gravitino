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

import uuid

from gravitino import Catalog, Fileset, NameIdentifier
from gravitino.api.authorization.privileges import Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from tests.integration.containers.hdfs_container import HDFSContainer
from tests.integration.integration_test_env import AuthorizationIntegrationTestEnv


class TestSupportsRoles(AuthorizationIntegrationTestEnv):
    _metalake_name = f"test_supports_roles_{uuid.uuid4().hex[:8]}"
    _metalake_comment = "test metadata object role operations"
    _fileset_catalog_name = "fileset_catalog"
    _fileset_schema_name = "fileset_schema"
    _fileset_name = "fileset"
    _relational_catalog_name = "relational_catalog"
    _relational_schema_name = "relational_schema"
    _table_name = "table"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._hdfs_container = HDFSContainer()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls._hdfs_container.close()
        finally:
            super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        self._metalake = self._gravitino_client.get_metalake()
        self._fileset_catalog = self._gravitino_client.create_catalog(
            name=self._fileset_catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=None,
            comment="test fileset catalog",
            properties={"location": f"/tmp/{self._metalake_name}/test_supports_roles"},
        )
        self._fileset_schema = self._fileset_catalog.as_schemas().create_schema(
            schema_name=self._fileset_schema_name,
            comment="test fileset schema",
            properties={},
        )
        self._fileset = self._fileset_catalog.as_fileset_catalog().create_fileset(
            ident=NameIdentifier.of(self._fileset_schema_name, self._fileset_name),
            comment="test fileset",
            fileset_type=Fileset.Type.MANAGED,
            storage_location=f"/tmp/{self._metalake_name}/test_supports_roles/fileset",
            properties={},
        )
        self._relational_catalog = self._gravitino_client.create_catalog(
            name=self._relational_catalog_name,
            catalog_type=Catalog.Type.RELATIONAL,
            provider="hive",
            comment="test relational catalog",
            properties={
                "metastore.uris": f"thrift://{self._hdfs_container.get_ip()}:9083"
            },
        )
        self._relational_schema = self._relational_catalog.as_schemas().create_schema(
            schema_name=self._relational_schema_name,
            comment="test relational schema",
            properties={},
        )
        self._table = self._relational_catalog.as_table_catalog().create_table(
            identifier=NameIdentifier.of(
                self._relational_schema_name, self._table_name
            ),
            columns=[
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
        )

    def test_list_binding_roles_for_metadata_objects(self) -> None:
        catalog_object = SecurableObjects.of_catalog(
            self._fileset_catalog_name, [Privileges.allow("USE_CATALOG")]
        )
        schema_object = SecurableObjects.of_schema(
            catalog_object,
            self._fileset_schema_name,
            [Privileges.allow("USE_SCHEMA")],
        )
        fileset_object = SecurableObjects.of_fileset(
            schema_object, self._fileset_name, [Privileges.allow("READ_FILESET")]
        )
        relational_catalog_object = SecurableObjects.of_catalog(
            self._relational_catalog_name, [Privileges.allow("USE_CATALOG")]
        )
        relational_schema_object = SecurableObjects.of_schema(
            relational_catalog_object,
            self._relational_schema_name,
            [Privileges.allow("USE_SCHEMA")],
        )
        table_object = SecurableObjects.of_table(
            relational_schema_object,
            self._table_name,
            [Privileges.allow("SELECT_TABLE")],
        )

        bindings = [
            (
                "metalake_role",
                SecurableObjects.of_metalake(
                    self._metalake_name, [Privileges.allow("CREATE_CATALOG")]
                ),
                self._metalake,
            ),
            ("catalog_role", catalog_object, self._fileset_catalog),
            ("schema_role", schema_object, self._fileset_schema),
            ("fileset_role", fileset_object, self._fileset),
            ("table_role", table_object, self._table),
        ]

        for role_name, securable_object, metadata_object in bindings:
            with self.subTest(metadata_type=securable_object.type()):
                self.assertEqual(
                    [], metadata_object.supports_roles().list_binding_role_names()
                )
                self._gravitino_client.create_role(
                    role_name, securable_objects=[securable_object]
                )
                self.assertEqual(
                    [role_name],
                    metadata_object.supports_roles().list_binding_role_names(),
                )
