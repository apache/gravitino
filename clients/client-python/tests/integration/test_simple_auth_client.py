"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging
import os
import unittest
from random import randint
from typing import Dict

from gravitino import (
    NameIdentifier,
    GravitinoClient,
    GravitinoAdminClient,
    Catalog,
    Fileset,
)
from gravitino.auth.simple_auth_provider import SimpleAuthProvider

logger = logging.getLogger(__name__)


class TestSimpleAuthClient(unittest.TestCase):
    creator: str = "test_client"
    metalake_name: str = "TestClient_metalake" + str(randint(1, 10000))
    catalog_name: str = "fileset_catalog"
    catalog_location_prop: str = "location"  # Fileset Catalog must set `location`
    catalog_provider: str = "hadoop"

    schema_name: str = "fileset_schema"

    fileset_name: str = "test_client_fileset"
    fileset_comment: str = "fileset_comment"

    fileset_location: str = "/tmp/TestFilesetCatalog"
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

    def setUp(self):
        os.environ["GRAVITINO_USER"] = self.creator
        self.gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090", auth_data_provider=SimpleAuthProvider()
        )
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):
        try:
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
            uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            auth_data_provider=SimpleAuthProvider(),
        )
        catalog = self.gravitino_client.create_catalog(
            ident=self.catalog_ident,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment="",
            properties={self.catalog_location_prop: "/tmp/test1"},
        )
        catalog.as_schemas().create_schema(
            ident=self.schema_ident, comment="", properties={}
        )
        catalog.as_fileset_catalog().create_fileset(
            ident=self.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=self.fileset_location,
            properties=self.fileset_properties,
        )

    def test_metalake_creator(self):
        metalake = self.gravitino_admin_client.load_metalake(
            NameIdentifier.of_metalake(self.metalake_ident.name())
        )
        self.assertEqual(metalake.audit_info().creator(), self.creator)

    def test_catalog_creator(self):
        catalog = self.gravitino_client.load_catalog(self.catalog_ident)
        self.assertEqual(catalog.audit_info().creator(), self.creator)

    def test_schema_creator(self):
        catalog = self.gravitino_client.load_catalog(self.catalog_ident)
        schema = catalog.as_schemas().load_schema(self.schema_ident)
        self.assertEqual(schema.audit_info().creator(), self.creator)

    def test_fileset_creator(self):
        catalog = self.gravitino_client.load_catalog(self.catalog_ident)
        fileset = catalog.as_fileset_catalog().load_fileset(self.fileset_ident)
        self.assertEqual(fileset.audit_info().creator(), self.creator)
