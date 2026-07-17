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
import os
from random import randint

from gravitino import (
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
)
from gravitino.api.authorization.owner import Owner
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    NoSuchMetadataObjectException,
    NotFoundException,
)

from tests.integration.integration_test_env import IntegrationTestEnv, MetalakeTestMixin

logger = logging.getLogger(__name__)


class TestOwner(MetalakeTestMixin, IntegrationTestEnv):
    metalake_name: str = "test_owner_metalake" + str(randint(1, 10000))
    catalog_name: str = "test_owner_catalog" + str(randint(1, 10000))
    test_user: str = "test_owner_user"

    gravitino_admin_client: GravitinoAdminClient = None
    gravitino_client: GravitinoClient = None

    @classmethod
    def setUpClass(cls):
        cls._get_gravitino_home()
        conf_path = os.path.join(cls.gravitino_home, "conf", "gravitino.conf")
        cls._reset_conf({"gravitino.authorization.enable": "true"}, conf_path)
        cls._append_conf({"gravitino.authorization.enable": "true"}, conf_path)
        if cls.use_external_gravitino():
            cls.restart_server()
        else:
            super().setUpClass()
        cls.gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")

    @classmethod
    def tearDownClass(cls):
        conf_path = os.path.join(cls.gravitino_home, "conf", "gravitino.conf")
        cls._reset_conf({"gravitino.authorization.enable": "false"}, conf_path)
        if cls.use_external_gravitino():
            cls.restart_server()
        else:
            super().tearDownClass()

    def create_catalog(self, catalog_name) -> Catalog:
        return self.gravitino_client.create_catalog(
            name=catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider="hadoop",
            comment="test owner catalog",
            properties={"location": "/tmp/test_owner"},
        )

    def clean_test_data(self):
        self.gravitino_client = self.create_gravitino_client(self.metalake_name)
        try:
            self.gravitino_client.drop_catalog(name=self.catalog_name, force=True)
        except GravitinoRuntimeException:
            logger.warning("Failed to drop catalog %s", self.catalog_name)

        self.drop_test_metalake(self.gravitino_admin_client, self.metalake_name)

    def test_get_owner_metalake(self):
        metalake_obj = MetadataObjects.of(
            [self.metalake_name], MetadataObject.Type.METALAKE
        )
        owner = self.gravitino_client.get_owner(metalake_obj)
        self.assertIsNotNone(owner)
        self.assertTrue(len(owner.name()) > 0)
        self.assertEqual(Owner.Type.USER, owner.type())

    def test_set_owner_metalake(self):
        self.gravitino_client.add_user(self.test_user)

        metalake_obj = MetadataObjects.of(
            [self.metalake_name], MetadataObject.Type.METALAKE
        )
        self.gravitino_client.set_owner(metalake_obj, self.test_user, Owner.Type.USER)

        owner = self.gravitino_client.get_owner(metalake_obj)
        self.assertIsNotNone(owner)
        self.assertEqual(self.test_user, owner.name())
        self.assertEqual(Owner.Type.USER, owner.type())

    def test_owner_catalog_and_schema(self):
        catalog = self.create_catalog(self.catalog_name)
        self.gravitino_client.add_user(self.test_user)

        # Test catalog-level owner
        catalog_obj = MetadataObjects.of(
            [self.catalog_name], MetadataObject.Type.CATALOG
        )
        owner = self.gravitino_client.get_owner(catalog_obj)
        self.assertIsNotNone(owner)
        self.assertTrue(len(owner.name()) > 0)
        self.assertEqual(Owner.Type.USER, owner.type())

        self.gravitino_client.set_owner(catalog_obj, self.test_user, Owner.Type.USER)
        owner = self.gravitino_client.get_owner(catalog_obj)
        self.assertEqual(self.test_user, owner.name())
        self.assertEqual(Owner.Type.USER, owner.type())

        # Test schema-level owner
        schema_name = "test_owner_schema"
        catalog.as_schemas().create_schema(schema_name, "comment", {})

        schema_obj = MetadataObjects.of(
            [self.catalog_name, schema_name], MetadataObject.Type.SCHEMA
        )
        self.gravitino_client.set_owner(schema_obj, self.test_user, Owner.Type.USER)

        owner = self.gravitino_client.get_owner(schema_obj)
        self.assertIsNotNone(owner)
        self.assertEqual(self.test_user, owner.name())
        self.assertEqual(Owner.Type.USER, owner.type())

    def test_owner_not_found(self):
        # Get owner for a non-existent catalog
        fake_catalog_obj = MetadataObjects.of(
            ["non_existent_catalog"], MetadataObject.Type.CATALOG
        )
        with self.assertRaises(NoSuchMetadataObjectException):
            self.gravitino_client.get_owner(fake_catalog_obj)

        # Set owner with a non-existent user
        metalake_obj = MetadataObjects.of(
            [self.metalake_name], MetadataObject.Type.METALAKE
        )
        with self.assertRaises(NotFoundException):
            self.gravitino_client.set_owner(
                metalake_obj, "non_existent_user", Owner.Type.USER
            )
