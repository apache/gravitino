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
from typing import Dict, List

from gravitino import (
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
    Fileset,
    FilesetChange,
)
from gravitino.audit.caller_context import CallerContext, CallerContextHolder
from gravitino.audit.fileset_audit_constants import FilesetAuditConstants
from gravitino.audit.fileset_data_operation import FilesetDataOperation
from gravitino.exceptions.base import (
    NoSuchFilesetException,
    GravitinoRuntimeException,
)
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestFilesetCatalog(IntegrationTestEnv):
    metalake_name: str = "TestFilesetCatalog_metalake" + str(randint(1, 10000))
    catalog_name: str = "catalog"
    catalog_location_prop: str = "location"  # Fileset Catalog must set `location`
    catalog_provider: str = "hadoop"

    schema_name: str = "schema"

    fileset_name: str = "fileset"
    fileset_alter_name: str = fileset_name + "Alter"
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
    fileset_new_name = fileset_name + "_new"

    catalog_ident: NameIdentifier = NameIdentifier.of(metalake_name, catalog_name)
    schema_ident: NameIdentifier = NameIdentifier.of(
        metalake_name, catalog_name, schema_name
    )
    fileset_ident: NameIdentifier = NameIdentifier.of(schema_name, fileset_name)
    fileset_new_ident: NameIdentifier = NameIdentifier.of(schema_name, fileset_new_name)

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    gravitino_client: GravitinoClient = None

    def setUp(self):
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):

        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        try:
            logger.info(
                "Drop fileset %s[%s]",
                self.fileset_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop fileset %s", self.fileset_ident)

        try:
            logger.info(
                "Drop fileset %s[%s]",
                self.fileset_new_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_new_ident),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop fileset %s", self.fileset_new_ident)

        try:
            logger.info(
                "Drop schema %s[%s]",
                self.schema_ident,
                catalog.as_schemas().drop_schema(
                    schema_name=self.schema_name, cascade=True
                ),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop schema %s", self.schema_name)

        try:
            logger.info(
                "Drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(name=self.catalog_name, force=True),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop catalog %s", self.catalog_name)

        try:
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_name,
                self.gravitino_admin_client.drop_metalake(
                    self.metalake_name, force=True
                ),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop metalake %s", self.metalake_name)

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        catalog = self.gravitino_client.create_catalog(
            name=self.catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=self.catalog_provider,
            comment="",
            properties={self.catalog_location_prop: "/tmp/test1"},
        )
        catalog.as_schemas().create_schema(
            schema_name=self.schema_name, comment="", properties={}
        )

    def create_fileset(self) -> Fileset:
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        return catalog.as_fileset_catalog().create_fileset(
            ident=self.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=self.fileset_location,
            properties=self.fileset_properties,
        )

    def create_custom_fileset(
        self, ident: NameIdentifier, storage_location: str
    ) -> Fileset:
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        return catalog.as_fileset_catalog().create_fileset(
            ident=ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=self.fileset_comment,
            storage_location=storage_location,
            properties=self.fileset_properties,
        )

    def test_create_fileset(self):
        fileset = self.create_fileset()
        self.assertIsNotNone(fileset)
        self.assertEqual(fileset.type(), Fileset.Type.MANAGED)
        self.assertEqual(fileset.comment(), self.fileset_comment)
        self.assertEqual(fileset.properties(), self.fileset_properties)

    def test_drop_fileset(self):
        self.create_fileset()
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        self.assertTrue(
            catalog.as_fileset_catalog().drop_fileset(ident=self.fileset_ident)
        )

    def test_list_fileset(self):
        self.create_fileset()
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        fileset_list: List[NameIdentifier] = catalog.as_fileset_catalog().list_filesets(
            namespace=self.fileset_ident.namespace()
        )
        self.assertTrue(any(item.name() == self.fileset_name for item in fileset_list))

    def test_load_fileset(self):
        self.create_fileset()
        fileset = (
            self.gravitino_client.load_catalog(name=self.catalog_name)
            .as_fileset_catalog()
            .load_fileset(ident=self.fileset_ident)
        )
        self.assertIsNotNone(fileset)
        self.assertEqual(fileset.name(), self.fileset_name)
        self.assertEqual(fileset.comment(), self.fileset_comment)
        self.assertEqual(fileset.properties(), self.fileset_properties)
        self.assertEqual(fileset.audit_info().creator(), "anonymous")

    def test_failed_load_fileset(self):
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        with self.assertRaises(NoSuchFilesetException):
            _ = catalog.as_fileset_catalog().load_fileset(ident=self.fileset_ident)

    def test_alter_fileset(self):
        self.create_fileset()
        fileset_properties_new_value = self.fileset_properties_value2 + "_new"
        fileset_new_comment = self.fileset_comment + "_new"

        changes = (
            FilesetChange.remove_property(self.fileset_properties_key1),
            FilesetChange.set_property(
                self.fileset_properties_key2, fileset_properties_new_value
            ),
            FilesetChange.update_comment(fileset_new_comment),
        )
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        fileset_new = catalog.as_fileset_catalog().alter_fileset(
            self.fileset_ident, *changes
        )
        self.assertEqual(
            fileset_new.properties().get(self.fileset_properties_key2),
            fileset_properties_new_value,
        )
        self.assertTrue(self.fileset_properties_key1 not in fileset_new.properties())
        self.assertEqual(fileset_new.comment(), fileset_new_comment)

        fileset_comment_removed = catalog.as_fileset_catalog().alter_fileset(
            self.fileset_ident, FilesetChange.remove_comment()
        )
        self.assertEqual(fileset_comment_removed.name(), self.fileset_name)
        self.assertIsNone(fileset_comment_removed.comment())

    def test_get_file_location(self):
        fileset_ident: NameIdentifier = NameIdentifier.of(
            self.schema_name, "test_get_file_location"
        )
        fileset_location = "/tmp/test_get_file_location"
        self.create_custom_fileset(fileset_ident, fileset_location)
        actual_file_location = (
            self.gravitino_client.load_catalog(name=self.catalog_name)
            .as_fileset_catalog()
            .get_file_location(fileset_ident, "/test/test.txt")
        )

        self.assertEqual(actual_file_location, f"file:{fileset_location}/test/test.txt")

        # test rename without sub path should throw an exception
        caller_context = CallerContext(
            {
                FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION: FilesetDataOperation.RENAME.name
            }
        )
        with self.assertRaises(GravitinoRuntimeException):
            CallerContextHolder.set(caller_context)
            (
                self.gravitino_client.load_catalog(name=self.catalog_name)
                .as_fileset_catalog()
                .get_file_location(fileset_ident, "")
            )
