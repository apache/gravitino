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

import logging
from random import randint

from gravitino import (
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
)
from gravitino.api.catalog_change import CatalogChange
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    CatalogAlreadyExistsException,
    NoSuchCatalogException,
)

from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestCatalog(IntegrationTestEnv):
    metalake_name: str = "TestSchema_metalake" + str(randint(1, 10000))

    catalog_name: str = "testCatalog" + str(randint(1, 10000))
    catalog_name_bak = catalog_name
    catalog_comment: str = "catalogComment"
    catalog_location_prop: str = "location"  # Fileset Catalog must set `location`
    catalog_provider: str = "hadoop"
    catalog_in_use_prop: str = "in-use"

    catalog_ident: NameIdentifier = NameIdentifier.of(metalake_name, catalog_name)

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    gravitino_client: GravitinoClient = None

    def setUp(self):
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )

    def create_catalog(self, catalog_name) -> Catalog:
        return self.gravitino_client.create_catalog(
            name=catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment=self.catalog_comment,
            properties={self.catalog_location_prop: "/tmp/test_schema"},
        )

    def clean_test_data(self):
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        try:
            logger.info(
                "TestCatalog: drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(name=self.catalog_name, force=True),
            )
        except GravitinoRuntimeException:
            logger.warning("TestCatalog: failed to drop catalog %s", self.catalog_name)

        try:
            logger.info(
                "TestCatalog: drop metalake %s[%s]",
                self.metalake_name,
                self.gravitino_admin_client.drop_metalake(
                    self.metalake_name, force=True
                ),
            )
        except GravitinoRuntimeException:
            logger.warning(
                "TestCatalog: failed to drop metalake %s", self.metalake_name
            )

        self.catalog_name = self.catalog_name_bak

    def test_list_catalogs(self):
        self.create_catalog(self.catalog_name)
        catalog_names = self.gravitino_client.list_catalogs()
        self.assertTrue(self.catalog_name in catalog_names)

    def test_create_catalog(self):
        catalog = self.create_catalog(self.catalog_name)
        self.assertEqual(catalog.name(), self.catalog_name)
        self.assertEqual(
            catalog.properties(),
            {
                self.catalog_location_prop: "/tmp/test_schema",
                self.catalog_in_use_prop: "true",
            },
        )

    def test_failed_create_catalog(self):
        self.create_catalog(self.catalog_name)
        with self.assertRaises(CatalogAlreadyExistsException):
            _ = self.create_catalog(self.catalog_name)

    def test_alter_catalog(self):
        catalog = self.create_catalog(self.catalog_name)

        catalog_new_name = self.catalog_name + "_new"
        catalog_new_comment = self.catalog_comment + "_new"
        catalog_properties_new_value = self.catalog_location_prop + "_new"
        catalog_properties_new_key: str = "catalog_properties_new_key"

        changes = (
            CatalogChange.rename(catalog_new_name),
            CatalogChange.update_comment(catalog_new_comment),
            CatalogChange.set_property(
                catalog_properties_new_key, catalog_properties_new_value
            ),
        )

        catalog = self.gravitino_client.alter_catalog(self.catalog_name, *changes)
        self.assertEqual(catalog.name(), catalog_new_name)
        self.assertEqual(catalog.comment(), catalog_new_comment)
        self.assertEqual(
            catalog.properties().get(catalog_properties_new_key),
            catalog_properties_new_value,
        )
        self.catalog_name = self.catalog_name + "_new"

    def test_drop_catalog(self):
        self.create_catalog(self.catalog_name)
        self.gravitino_client.disable_catalog(self.catalog_name)
        self.assertTrue(self.gravitino_client.drop_catalog(name=self.catalog_name))

    def test_list_catalogs_info(self):
        self.create_catalog(self.catalog_name)
        catalogs_info = self.gravitino_client.list_catalogs_info()
        self.assertTrue(any(item.name() == self.catalog_name for item in catalogs_info))

    def test_load_catalog(self):
        self.create_catalog(self.catalog_name)
        catalog = self.gravitino_client.load_catalog(self.catalog_name)
        self.assertIsNotNone(catalog)
        self.assertEqual(catalog.name(), self.catalog_name)
        self.assertEqual(catalog.comment(), self.catalog_comment)
        self.assertEqual(
            catalog.properties(),
            {
                self.catalog_location_prop: "/tmp/test_schema",
                self.catalog_in_use_prop: "true",
            },
        )
        self.assertEqual(catalog.audit_info().creator(), "anonymous")

    def test_failed_load_catalog(self):
        with self.assertRaises(NoSuchCatalogException):
            self.gravitino_client.load_catalog(self.catalog_name)
