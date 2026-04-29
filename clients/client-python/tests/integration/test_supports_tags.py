# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import typing as tp
from random import randint

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, GravitinoMetalake
from gravitino.api.file.fileset import Fileset
from gravitino.api.model.model import Model
from gravitino.api.rel.table import Table
from gravitino.api.rel.table_catalog import TableCatalog
from gravitino.api.rel.types.types import Types
from gravitino.api.tag import Tag
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.exceptions.base import NoSuchTagException
from gravitino.name_identifier import NameIdentifier
from tests.integration.containers.hdfs_container import HDFSContainer
from tests.integration.integration_test_env import IntegrationTestEnv

# pylint: disable=too-many-instance-attributes


class TestSupportsTags(IntegrationTestEnv):
    relational_catalog_provider: str = "hive"
    fileset_comment: str = "fileset_comment"
    catalog_location_prop: str = "location"

    fileset_location: str = "/tmp/TestFilesetCatalog"
    fileset_properties_key1: str = "fileset_properties_key1"
    fileset_properties_value1: str = "fileset_properties_value1"
    fileset_properties_key2: str = "fileset_properties_key2"
    fileset_properties_value2: str = "fileset_properties_value2"
    fileset_properties: tp.Dict[str, str] = {
        fileset_properties_key1: fileset_properties_value1,
        fileset_properties_key2: fileset_properties_value2,
    }
    multiple_locations_fileset_properties: tp.Dict[str, str] = {
        Fileset.PROPERTY_DEFAULT_LOCATION_NAME: "location1",
        **fileset_properties,
    }

    _metalake_name: str = "tag_it_metalake" + str(randint(0, 1000))
    _relational_catalog_name: str = "relational_catalog" + str(randint(0, 1000))
    _model_catalog_name: str = "model_catalog" + str(randint(0, 1000))
    _fileset_catalog_name: str = "fileset_catalog" + str(randint(0, 1000))
    # SCHEMA
    _schema_name = "tag_it_schema" + str(randint(0, 1000))
    # OTHER
    _table_name: str = "tag_it_table" + str(randint(0, 1000))
    _fileset_name: str = "tag_it_fileset" + str(randint(0, 1000))
    _model_name: str = "tag_it_model" + str(randint(0, 1000))

    _tag_name1: str = "tag_it_tag1" + str(randint(0, 1000))
    _tag_name2: str = "tag_it_tag2" + str(randint(0, 1000))
    _tag_name3: str = "tag_it_tag3" + str(randint(0, 1000))
    _tag_name4: str = "tag_it_tag4" + str(randint(0, 1000))

    _gravitino_admin_client: GravitinoAdminClient
    _gravitino_client: GravitinoClient
    _metalake: GravitinoMetalake
    _model_catalog: Catalog
    _table_catalog: TableCatalog
    _relational_catalog: Catalog
    _fileset_catalog: Catalog
    _tag1: Tag
    _tag2: Tag
    _tag3: Tag
    _tag4: Tag

    _table_ident: NameIdentifier
    _fileset_ident: NameIdentifier
    _model_ident: NameIdentifier
    _hdfs_container: HDFSContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._hdfs_container = HDFSContainer()
        cls._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")

        cls._metalake = cls._gravitino_admin_client.create_metalake(
            cls._metalake_name, comment="test metalake", properties={}
        )
        cls._gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls._metalake_name
        )

        cls._gravitino_client.create_tag(cls._tag_name1, "test tag1", {})
        cls._gravitino_client.create_tag(cls._tag_name2, "test tag2", {})
        cls._gravitino_client.create_tag(cls._tag_name3, "test tag3", {})
        cls._gravitino_client.create_tag(cls._tag_name4, "test tag4", {})

        cls._tag1 = cls._gravitino_client.get_tag(cls._tag_name1)
        cls._tag2 = cls._gravitino_client.get_tag(cls._tag_name2)
        cls._tag3 = cls._gravitino_client.get_tag(cls._tag_name3)
        cls._tag4 = cls._gravitino_client.get_tag(cls._tag_name4)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._gravitino_client.drop_catalog(name=cls._model_catalog_name, force=True)
        cls._gravitino_client.drop_catalog(
            name=cls._relational_catalog_name, force=True
        )
        cls._gravitino_client.drop_catalog(name=cls._fileset_catalog_name, force=True)

        cls._gravitino_client.delete_tag(cls._tag_name1)
        cls._gravitino_client.delete_tag(cls._tag_name2)
        cls._gravitino_client.delete_tag(cls._tag_name3)
        cls._gravitino_client.delete_tag(cls._tag_name4)

        cls._gravitino_admin_client.drop_metalake(name=cls._metalake_name, force=True)

    def setUp(self) -> None:
        hive_metastore_uri = f"thrift://{self._hdfs_container.get_ip()}:9083"
        self._model_catalog = self._gravitino_client.create_catalog(
            name=self._model_catalog_name,
            catalog_type=Catalog.Type.MODEL,
            provider="",
            comment="comment",
            properties={},
        )
        self._fileset_catalog = self._gravitino_client.create_catalog(
            name=self._fileset_catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider="",
            comment="",
            properties={self.catalog_location_prop: "/tmp/test1"},
        )
        self._relational_catalog = self._gravitino_client.create_catalog(
            name=self._relational_catalog_name,
            catalog_type=Catalog.Type.RELATIONAL,
            provider=self.relational_catalog_provider,
            comment="Test relational catalog",
            properties={"metastore.uris": hive_metastore_uri},
        )
        self._table_catalog = self._relational_catalog.as_table_catalog()
        self._model_catalog.as_schemas().create_schema(
            self._schema_name, "model schema comment", {}
        )
        self._relational_catalog.as_schemas().create_schema(
            self._schema_name, "relational schema comment", {}
        )
        self._fileset_catalog.as_schemas().create_schema(
            self._schema_name, "fileset schema comment", {}
        )
        self._table_ident: NameIdentifier = NameIdentifier.of(
            self._metalake_name,
            self._relational_catalog_name,
            self._schema_name,
            self._table_name,
        )
        self._fileset_ident: NameIdentifier = NameIdentifier.of(
            self._metalake_name,
            self._fileset_catalog_name,
            self._schema_name,
            self._fileset_name,
        )
        self._model_ident = NameIdentifier.of(
            self._metalake_name,
            self._model_catalog_name,
            self._schema_name,
            self._model_name,
        )

    def tearDown(self) -> None:
        self._model_catalog.as_schemas().drop_schema(self._schema_name, True)
        self._relational_catalog.as_schemas().drop_schema(self._schema_name, True)
        self._fileset_catalog.as_schemas().drop_schema(self._schema_name, True)

    def test_catalog_tag_operations(self) -> None:
        """
        Test tag operations (associate, list, get, dissociate) on catalog.
        """
        model_catalog = self._model_catalog.supports_tags()

        model_catalog.associate_tags(
            tags_to_add=[self._tag_name3, self._tag_name4],
            tags_to_remove=[],
        )
        model_catalog.associate_tags(
            tags_to_add=[self._tag_name1, self._tag_name2],
            tags_to_remove=[self._tag_name3, self._tag_name4],
        )
        self.test_list_tags(model_catalog)
        self.test_list_tags_info(model_catalog)
        self.test_get_tag(model_catalog)
        model_catalog.associate_tags(
            tags_to_add=[],
            tags_to_remove=[self._tag_name1, self._tag_name2],
        )
        self._check_no_tag_associated(model_catalog)

    def test_schema_tag_operations(self) -> None:
        """
        Test tag operations (associate, list, get, dissociate) on schema.
        """
        schema = self._model_catalog.as_schemas().load_schema(self._schema_name)

        schema_supports_tags = schema.supports_tags()
        schema_supports_tags.associate_tags(
            tags_to_add=[self._tag_name3, self._tag_name4],
            tags_to_remove=[],
        )
        schema_supports_tags.associate_tags(
            tags_to_add=[self._tag_name1, self._tag_name2],
            tags_to_remove=[self._tag_name3, self._tag_name4],
        )
        self.test_list_tags(schema_supports_tags)
        self.test_list_tags_info(schema_supports_tags)
        self.test_get_tag(schema_supports_tags)

        schema.supports_tags().associate_tags(
            tags_to_add=[],
            tags_to_remove=[self._tag_name1, self._tag_name2],
        )
        self._check_no_tag_associated(schema.supports_tags())

    def test_fileset_tag_operations(self) -> None:
        """
        Test tag operations (associate, list, get, dissociate) on fileset.
        """
        fileset: Fileset = self._fileset_catalog.as_fileset_catalog().create_fileset(
            ident=self._fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=self.fileset_location,
            properties=self.fileset_properties,
        )
        fileset.supports_tags().associate_tags(
            tags_to_add=[self._tag_name3, self._tag_name4],
            tags_to_remove=[],
        )
        fileset.supports_tags().associate_tags(
            tags_to_add=[self._tag_name1, self._tag_name2],
            tags_to_remove=[self._tag_name3, self._tag_name4],
        )
        self.test_list_tags(fileset.supports_tags())
        self.test_list_tags_info(fileset.supports_tags())
        self.test_get_tag(fileset.supports_tags())

        fileset.supports_tags().associate_tags(
            tags_to_add=[],
            tags_to_remove=[self._tag_name1, self._tag_name2],
        )
        self._check_no_tag_associated(fileset.supports_tags())

    def test_model_tag_operations(self) -> None:
        """
        Test tag operations (associate, list, get, dissociate) on model.
        """

        model: Model = self._model_catalog.as_model_catalog().register_model(
            self._model_ident, "test_model", {}
        )

        model.supports_tags().associate_tags(
            tags_to_add=[self._tag_name3, self._tag_name4],
            tags_to_remove=[],
        )
        model.supports_tags().associate_tags(
            tags_to_add=[self._tag_name1, self._tag_name2],
            tags_to_remove=[self._tag_name3, self._tag_name4],
        )
        self.test_list_tags(model.supports_tags())
        self.test_list_tags_info(model.supports_tags())
        self.test_get_tag(model.supports_tags())
        model.supports_tags().associate_tags(
            tags_to_add=[],
            tags_to_remove=[self._tag_name1, self._tag_name2],
        )
        self._check_no_tag_associated(model.supports_tags())

    def test_table_tag_operations(self) -> None:
        """Test tag operations (associate, list, get, dissociate) on table."""
        relational_table = self.create_test_table()

        relational_table.supports_tags().associate_tags(
            tags_to_add=[self._tag_name3, self._tag_name4],
            tags_to_remove=[],
        )
        relational_table.supports_tags().associate_tags(
            tags_to_add=[self._tag_name1, self._tag_name2],
            tags_to_remove=[self._tag_name3, self._tag_name4],
        )
        self.test_list_tags(relational_table.supports_tags())
        self.test_list_tags_info(relational_table.supports_tags())
        self.test_get_tag(relational_table.supports_tags())

        relational_table.supports_tags().associate_tags(
            tags_to_add=[],
            tags_to_remove=[self._tag_name1, self._tag_name2],
        )
        self._check_no_tag_associated(relational_table.supports_tags())

    def test_column_tag_operations(self) -> None:
        """Test tag operations (associate, list, get, dissociate) on column."""
        relational_table = self.create_test_table()
        for column in relational_table.columns():
            column.supports_tags().associate_tags(
                tags_to_add=[self._tag_name3, self._tag_name4],
                tags_to_remove=[],
            )
            column.supports_tags().associate_tags(
                tags_to_add=[self._tag_name1, self._tag_name2],
                tags_to_remove=[self._tag_name3, self._tag_name4],
            )
            self.test_list_tags(column.supports_tags())
            self.test_list_tags_info(column.supports_tags())
            self.test_get_tag(column.supports_tags())

            column.supports_tags().associate_tags(
                tags_to_add=[],
                tags_to_remove=[self._tag_name1, self._tag_name2],
            )
            self._check_no_tag_associated(column.supports_tags())

    def test_get_non_existent_tag_raises_exception(self) -> None:
        """
        Test get tag on non-existent tag.
        """
        self._model_catalog.supports_tags().associate_tags(
            tags_to_add=[self._tag_name3, self._tag_name4],
            tags_to_remove=[],
        )

        with self.assertRaises(NoSuchTagException):
            self._model_catalog.supports_tags().get_tag(self._tag_name1)

    def test_get_tag(self, supports_tags: SupportsTags) -> None:
        tag1: Tag = supports_tags.get_tag(self._tag_name1)
        self._check_tag(self._tag1, tag1)

        tag2: Tag = supports_tags.get_tag(self._tag_name2)
        self._check_tag(self._tag2, tag2)

    def test_list_tags(self, supports_tags: SupportsTags) -> None:
        tags = supports_tags.list_tags()
        self.assertEqual(2, len(tags))
        self.assertIn(self._tag_name1, tags)
        self.assertIn(self._tag_name2, tags)

    def test_list_tags_info(self, supports_tags: SupportsTags) -> None:
        tags: list[Tag] = supports_tags.list_tags_info()
        self.assertEqual(2, len(tags))
        for tag in tags:
            if tag.name() == self._tag_name1:
                self._check_tag(self._tag1, tag)
            elif tag.name() == self._tag_name2:
                self._check_tag(self._tag2, tag)
            else:
                self.fail("Unknown tag name")

    def create_test_table(self) -> Table:
        return self._table_catalog.create_table(
            identifier=self._table_ident,
            columns=[
                ColumnDTO.builder()
                .with_name("dt")
                .with_data_type(Types.DateType.get())
                .build(),
                ColumnDTO.builder()
                .with_name("country")
                .with_data_type(Types.StringType.get())
                .build(),
            ],
            partitioning=[
                IdentityPartitioningDTO("dt"),
                IdentityPartitioningDTO("country"),
            ],
        )

    def _check_no_tag_associated(self, supports_tags: SupportsTags) -> None:
        self.assertEqual(0, len(supports_tags.list_tags()))
        self.assertListEqual([], supports_tags.list_tags_info())

    def _check_tag(self, expected_tag: Tag, real_tag: Tag) -> None:
        self.assertEqual(expected_tag.name(), real_tag.name())
        self.assertEqual(expected_tag.comment(), real_tag.comment())
        self.assertEqual(expected_tag.properties(), real_tag.properties())
        self.assertEqual(expected_tag.inherited(), real_tag.inherited())
