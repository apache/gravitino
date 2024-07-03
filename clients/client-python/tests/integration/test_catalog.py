"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import logging
from random import randint

from gravitino import (
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
)

from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestCatalog(IntegrationTestEnv):
    metalake_name: str = "TestSchema_metalake" + str(randint(1, 10000))

    catalog_name: str = "testCatalog"
    catalog_location_prop: str = "location"  # Fileset Catalog must set `location`
    catalog_provider: str = "hadoop"

    catalog_ident: NameIdentifier = NameIdentifier.of_catalog(
        metalake_name, catalog_name
    )

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
        self.gravitino_client.create_catalog(
            name=self.catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment="",
            properties={self.catalog_location_prop: "/tmp/test_schema"},
        )

    def clean_test_data(self):
        try:
            self.gravitino_client = GravitinoClient(
                uri="http://localhost:8090", metalake_name=self.metalake_name
            )
            logger.info(
                "Drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(name=self.catalog_name),
            )
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_name,
                self.gravitino_admin_client.drop_metalake(self.metalake_name),
            )
        except Exception as e:
            logger.error("Clean test data failed: %s", e)

    def test_list_catalogs(self):
        catalog_names = self.gravitino_client.list_catalogs()
        self.assertTrue(self.catalog_name in catalog_names)
