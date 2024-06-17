#  Copyright 2024 Datastrato Pvt Ltd.
#  This software is licensed under the Apache License version 2.
import logging
import os
import unittest
from random import randint

from typing import Dict
from gravitino import (
    gvfs,
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
    Fileset,
)

logger = logging.getLogger(__name__)


class TestGvfsWithHDFS(unittest.TestCase):
    metalake_name: str = "TestGvfsWithHDFS_metalake" + str(randint(1, 10000))
    catalog_name: str = "test_gvfs_catalog"
    catalog_provider: str = "hadoop"

    schema_name: str = "test_gvfs_schema"

    fileset_name: str = "test_gvfs_fileset"
    fileset_comment: str = "fileset_comment"

    fileset_properties_key1: str = "fileset_properties_key1"
    fileset_properties_value1: str = "fileset_properties_value1"
    fileset_properties_key2: str = "fileset_properties_key2"
    fileset_properties_value2: str = "fileset_properties_value2"
    fileset_properties: Dict[str, str] = {
        fileset_properties_key1: fileset_properties_value1,
        fileset_properties_key2: fileset_properties_value2,
    }

    metalake_ident: NameIdentifier = NameIdentifier.of(metalake_name)
    catalog_ident: NameIdentifier = NameIdentifier.of_catalog(
        metalake_name, catalog_name
    )
    schema_ident: NameIdentifier = NameIdentifier.of_schema(
        metalake_name, catalog_name, schema_name
    )
    fileset_ident: NameIdentifier = NameIdentifier.of_fileset(
        metalake_name, catalog_name, schema_name, fileset_name
    )

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    gravitino_client: GravitinoClient = None

    def setUp(self):
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):
        try:
            self.gravitino_client = GravitinoClient(
                uri="http://localhost:8090", metalake_name=self.metalake_name
            )
            catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
            logger.info(
                "Drop fileset %s[%s]",
                self.fileset_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident),
            )
            logger.info(
                "Drop schema %s[%s]",
                self.schema_ident,
                catalog.as_schemas().drop_schema(ident=self.schema_ident, cascade=True),
            )
            logger.info(
                "Drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(ident=self.catalog_ident),
            )
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_ident,
                self.gravitino_admin_client.drop_metalake(self.metalake_ident),
            )
        except Exception as e:
            logger.error("Clean test data failed: %s", e)

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            ident=self.metalake_ident, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        catalog = self.gravitino_client.create_catalog(
            ident=self.catalog_ident,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment="",
            properties={},
        )
        catalog.as_schemas().create_schema(
            ident=self.schema_ident, comment="", properties={}
        )

        self.hdfs_host = os.environ.get("GRAVITINO_PYTHON_HIVE_ADDRESS")
        self.assertIsNotNone(self.hdfs_host)
        self.fileset_storage_location: str = (
            f"hdfs://{self.hdfs_host}:9000/{self.catalog_name}/{self.schema_name}/{self.fileset_name}"
        )
        catalog.as_fileset_catalog().create_fileset(
            ident=self.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=self.fileset_storage_location,
            properties=self.fileset_properties,
        )

    def test_load_fileset(self):
        catalog = self.gravitino_client.load_catalog(ident=self.catalog_ident)
        self.assertIsNotNone(catalog)
        fileset = catalog.as_fileset_catalog().load_fileset(ident=self.fileset_ident)
        self.assertEqual(fileset.type(), Fileset.Type.MANAGED)
        self.assertEqual(fileset.comment(), self.fileset_comment)
        self.assertEqual(fileset.storage_location(), self.fileset_storage_location)
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        gvfs_path = (
            f"gvfs://fileset/{self.catalog_name}/{self.schema_name}/{self.fileset_name}"
        )
        self.assertTrue(fs.exists(gvfs_path))
