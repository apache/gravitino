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
import logging
import typing as tp
from random import randint

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, GravitinoMetalake
from gravitino.api.file.fileset import Fileset
from gravitino.api.rel.table_catalog import TableCatalog
from gravitino.api.rel.types.types import Types
from gravitino.api.tag import Tag
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.client.generic_model import GenericModel
from gravitino.client.generic_schema import GenericSchema
from gravitino.client.generic_tag import GenericTag
from gravitino.client.relational_table import RelationalTable
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.name_identifier import NameIdentifier
from tests.integration.containers.hdfs_container import HDFSContainer
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestSupportsJobs(IntegrationTestEnv):
    relational_catalog_provider: str = "hive"
    fileset_name: str = "fileset"
    multiple_locations_fileset_name: str = "multiple_locations_fileset"
    fileset_comment: str = "fileset_comment"
    catalog_location_prop: str = "location"

    fileset_location: str = "/tmp/TestFilesetCatalog"
    fileset_location2: str = "/tmp/TestFilesetCatalog2"
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
    _relational_catalog_name = "relational_catalog" + str(randint(0, 1000))
    _model_catalog_name = "model_catalog" + str(randint(0, 1000))
    _fileset_catalog_name: str = "fileset_catalog" + str(randint(0, 1000))
    # SCHEMA
    _schema_name = "tag_it_schema" + str(randint(0, 1000))
    # OTHER
    _table_name = "tag_it_table" + str(randint(0, 1000))

    _tag_name1: str = "tag_it_tag1" + str(randint(0, 1000))
    _tag_name2: str = "tag_it_tag2" + str(randint(0, 1000))

    _gravitino_admin_client: GravitinoAdminClient
    _gravitino_client: GravitinoClient
    _metalake: GravitinoMetalake
    _model_catalog: Catalog
    _table_catalog: TableCatalog
    _relation_catalog: Catalog
    _fileset_catalog: Catalog
    _tag1: GenericTag
    _tag2: GenericTag

    _table_ident: NameIdentifier = NameIdentifier.of(
        _metalake_name, _relational_catalog_name, _schema_name, _table_name
    )
    _hdfs_container: HDFSContainer

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._hdfs_container = HDFSContainer()
        hive_metastore_uri = f"thrift://{cls._hdfs_container.get_ip()}:9083"
        logger.info("Started Hive container with metastore URI: %s", hive_metastore_uri)

        cls._get_gravitino_home()
        cls.restart_server()

        cls._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")
        cls._gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls._metalake_name
        )
        cls._model_catalog = cls._gravitino_client.create_catalog(
            name=cls._model_catalog_name,
            catalog_type=Catalog.Type.MODEL,
            provider="",
            comment="comment",
            properties={},
        )
        cls._fileset_catalog = cls._gravitino_client.create_catalog(
            name=cls._fileset_catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider="",
            comment="",
            properties={cls.catalog_location_prop: "/tmp/test1"},
        )
        cls._relation_catalog = cls._gravitino_client.create_catalog(
            name=cls._relational_catalog_name,
            catalog_type=Catalog.Type.RELATIONAL,
            provider=cls.relational_catalog_provider,
            comment="Test relational catalog",
            properties={"metastore.uris": hive_metastore_uri},
        )
        cls._table_catalog = cls._relation_catalog.as_table_catalog()

        cls._gravitino_client.create_tag(cls._tag_name1, "test tag1", {})
        cls._gravitino_client.create_tag(cls._tag_name2, "test tag2", {})

        cls._tag1 = cls._gravitino_client.get_tag(cls._tag_name1)  # type: ignore
        cls._tag2 = cls._gravitino_client.get_tag(cls._tag_name2)  # type: ignore

    @classmethod
    def tearDownClass(cls) -> None:
        cls._gravitino_client.drop_catalog(name=cls._model_catalog_name, force=True)
        cls._gravitino_client.drop_catalog(
            name=cls._relational_catalog_name, force=True
        )
        cls._gravitino_client.drop_catalog(name=cls._fileset_catalog_name, force=True)

        cls._gravitino_client.delete_tag(cls._tag_name1)
        cls._gravitino_client.delete_tag(cls._tag_name2)
        cls._gravitino_admin_client.drop_metalake(name=cls._metalake_name, force=True)

    def setUp(self) -> None:
        self._model_catalog.as_schemas().create_schema(self._schema_name, "comment", {})
        self._relation_catalog.as_schemas().create_schema(
            self._schema_name, "relational schema comment", {}
        )
        self._model_catalog.as_model_catalog().create_schema(
            self._schema_name, "model schema comment", {}
        )
        self._fileset_catalog.as_schemas().create_schema(
            self._schema_name, "fileset schema comment", {}
        )

    def tearDown(self) -> None:
        self._model_catalog.as_schemas().drop_schema(self._schema_name, True)
        self._relation_catalog.as_schemas().drop_schema(self._schema_name, True)
        self._fileset_catalog.as_schemas().drop_schema(self._schema_name, True)

    def test_list_tag_info(self) -> None:
        schema: GenericSchema = self._model_catalog.as_schemas().load_schema(
            self._schema_name  # type: ignore
        )
        schema.associate_tags([self._tag_name1, self._tag_name2], [])
        infos = schema.list_tags_info()

        self.assertEqual(2, len(infos))
        for info in infos:
            self.assertTrue(info.name() in [self._tag_name1, self._tag_name2])
            self.assertTrue(info.comment() in [self, "test tag1", "test tag2"])
            self.assertEqual(info.properties(), {})

    def test_associate_tag_to_schema_and_get(self) -> None:
        schema: GenericSchema = self._model_catalog.as_schemas().load_schema(
            self._schema_name  # type: ignore
        )

        expected_tags = schema.associate_tags([self._tag_name1, self._tag_name2], [])
        real_associated_tags = schema.list_tags()
        self.assertEqual(expected_tags, real_associated_tags)

        retrieved_tag1: Tag = schema.get_tag(self._tag_name1)
        self._check_tag(self._tag1, retrieved_tag1)

        retrieved_tag2: Tag = schema.get_tag(self._tag_name2)
        self._check_tag(self._tag2, retrieved_tag2)

    def test_associate_tag_to_catalog_and_get(self) -> None:
        model_supports_tags_catalog = self._model_catalog.supports_tags()
        model_supports_tags_catalog.associate_tags(
            [self._tag_name1, self._tag_name2], []
        )

        self._test_get_tag(model_supports_tags_catalog)

    def test_associate_tag_to_column_and_get(self) -> None:
        pass

    def test_associate_tag_fileset_and_get(self) -> None:
        pass

    def test_associate_tag_to_model_and_get(self) -> None:
        model_ident = NameIdentifier.of(
            self._metalake_name, self._model_catalog_name, self._schema_name
        )
        model: GenericModel = self._model_catalog.as_model_catalog().register_model(  # type: ignore
            model_ident, "test_model", {}
        )
        expected_tags = model.associate_tags([self._tag_name1, self._tag_name2], [])
        real_associated_tags = model.list_tags()
        self.assertEqual(expected_tags, real_associated_tags)

        self._test_get_tag(model)

    def test_associate_tag_to_table_and_get(self) -> None:
        relational_table: RelationalTable = self._table_catalog.create_table(  # type: ignore
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

        relational_table.associate_tags([self._tag_name1, self._tag_name2], [])
        self._test_list_tags(relational_table)
        self._test_get_tag(relational_table)

    def _test_get_tag(self, supports_tags: SupportsTags) -> None:
        tag1: Tag = supports_tags.get_tag(self._tag_name1)
        self._check_tag(self._tag1, tag1)

        tag2: Tag = supports_tags.get_tag(self._tag_name2)
        self._check_tag(self._tag2, tag2)

    def _test_list_tags(self, supports_tags: SupportsTags) -> None:
        tags = supports_tags.list_tags()
        self.assertEqual(2, len(tags))
        self.assertIn(self._tag_name1, tags)
        self.assertIn(self._tag_name2, tags)

    def _test_list_tags_info(self, supports_tags: SupportsTags) -> None:
        # todo
        tags: list[Tag] = supports_tags.list_tags_info()
        self.assertEqual(2, len(tags))
        self.assertIn(self._tag_name1, tags)
        self.assertIn(self._tag_name2, tags)

    def _check_tag(self, expected_tag: Tag, real_tag: Tag) -> None:
        self.assertEqual(expected_tag.name(), real_tag.name())
        self.assertEqual(expected_tag.comment(), real_tag.comment())
        self.assertEqual(expected_tag.properties(), real_tag.properties())
        self.assertEqual(expected_tag.inherited(), real_tag.inherited())
