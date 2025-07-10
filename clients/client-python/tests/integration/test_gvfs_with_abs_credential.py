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
import unittest


from adlfs import AzureBlobFileSystem

from gravitino import (
    gvfs,
    GravitinoClient,
    Catalog,
    Fileset,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from tests.integration.test_gvfs_with_abs import TestGvfsWithABS


logger = logging.getLogger(__name__)


def azure_abs_with_credential_is_prepared():
    return (
        os.environ.get("ABS_ACCOUNT_NAME_FOR_CREDENTIAL")
        and os.environ.get("ABS_ACCOUNT_KEY_FOR_CREDENTIAL")
        and os.environ.get("ABS_CONTAINER_NAME_FOR_CREDENTIAL")
        and os.environ.get("ABS_TENANT_ID_FOR_CREDENTIAL")
        and os.environ.get("ABS_CLIENT_ID_FOR_CREDENTIAL")
        and os.environ.get("ABS_CLIENT_SECRET_FOR_CREDENTIAL")
    )


@unittest.skipUnless(
    azure_abs_with_credential_is_prepared(),
    "Azure Blob Storage credential test is not prepared.",
)
class TestGvfsWithCredentialABS(TestGvfsWithABS):
    # Before running this test, please set the make sure azure-bundle-xxx.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/fileset/libs/ directory
    azure_abs_account_key = os.environ.get("ABS_ACCOUNT_KEY_FOR_CREDENTIAL")
    azure_abs_account_name = os.environ.get("ABS_ACCOUNT_NAME_FOR_CREDENTIAL")
    azure_abs_container_name = os.environ.get("ABS_CONTAINER_NAME_FOR_CREDENTIAL")
    azure_abs_tenant_id = os.environ.get("ABS_TENANT_ID_FOR_CREDENTIAL")
    azure_abs_client_id = os.environ.get("ABS_CLIENT_ID_FOR_CREDENTIAL")
    azure_abs_client_secret = os.environ.get("ABS_CLIENT_SECRET_FOR_CREDENTIAL")

    metalake_name: str = "TestGvfsWithCredentialABS_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_NAME: self.azure_abs_account_name,
            GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_KEY: self.azure_abs_account_key,
            GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True,
        }

    @classmethod
    def _init_test_entities(cls):
        cls.gravitino_admin_client.create_metalake(
            name=cls.metalake_name, comment="", properties={}
        )
        cls.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls.metalake_name
        )

        cls.config = {}
        cls.conf = {}
        catalog = cls.gravitino_client.create_catalog(
            name=cls.catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=cls.catalog_provider,
            comment="",
            properties={
                "filesystem-providers": "abs",
                "azure-storage-account-name": cls.azure_abs_account_name,
                "azure-storage-account-key": cls.azure_abs_account_key,
                "azure-tenant-id": cls.azure_abs_tenant_id,
                "azure-client-id": cls.azure_abs_client_id,
                "azure-client-secret": cls.azure_abs_client_secret,
                "credential-providers": "adls-token",
            },
        )
        catalog.as_schemas().create_schema(
            schema_name=cls.schema_name, comment="", properties={}
        )

        cls.fileset_storage_location: str = (
            f"{cls.azure_abs_container_name}/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
        )
        cls.fileset_gvfs_location = (
            f"gvfs://fileset/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
        )
        catalog.as_fileset_catalog().create_fileset(
            ident=cls.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=cls.fileset_comment,
            storage_location=(
                f"abfss://{cls.azure_abs_container_name}@{cls.azure_abs_account_name}.dfs.core.windows.net/"
                f"{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
            ),
            properties=cls.fileset_properties,
        )

        cls.multiple_locations_fileset_storage_location: str = (
            f"abfss://{cls.azure_abs_container_name}@{cls.azure_abs_account_name}.dfs.core.windows.net/"
            f"{cls.catalog_name}/{cls.schema_name}/"
            f"{cls.multiple_locations_fileset_name}"
        )
        cls.multiple_locations_fileset_storage_location1: str = (
            f"abfss://{cls.azure_abs_container_name}@{cls.azure_abs_account_name}.dfs.core.windows.net/"
            f"{cls.catalog_name}/{cls.schema_name}/"
            f"{cls.multiple_locations_fileset_name}_1"
        )
        cls.multiple_locations_fileset_gvfs_location = (
            f"gvfs://fileset/{cls.catalog_name}/{cls.schema_name}/"
            f"{cls.multiple_locations_fileset_name}"
        )
        catalog.as_fileset_catalog().create_multiple_location_fileset(
            ident=cls.multiple_locations_fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=cls.fileset_comment,
            storage_locations={
                "default": cls.multiple_locations_fileset_storage_location,
                "location1": cls.multiple_locations_fileset_storage_location1,
            },
            properties={
                Fileset.PROPERTY_DEFAULT_LOCATION_NAME: "default",
                **cls.fileset_properties,
            },
        )

        cls.fs = AzureBlobFileSystem(
            account_name=cls.azure_abs_account_name,
            account_key=cls.azure_abs_account_key,
        )

    # As the permission provided by the dynamic token is smaller compared to the permission provided by the static token
    # like account key and account name, the test case will fail if we do not override the test case.
    def test_mkdir(self):
        mkdir_dir = self.fileset_gvfs_location + "/test_mkdir"
        mkdir_actual_dir = self.fileset_storage_location + "/test_mkdir"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )

        # it actually takes no effect.
        self.check_mkdir(mkdir_dir, mkdir_actual_dir, fs)

        # check whether it will automatically create the bucket if 'create_parents'
        # is set to True.
        new_bucket = self.azure_abs_container_name + "2"
        mkdir_actual_dir = mkdir_actual_dir.replace(
            self.azure_abs_container_name, new_bucket
        )
        self.fs.mkdir(mkdir_actual_dir, create_parents=True)

        self.assertFalse(self.fs.exists(mkdir_actual_dir))

        self.assertTrue(self.fs.exists(f"abfss://{new_bucket}"))

    def test_makedirs(self):
        mkdir_dir = self.fileset_gvfs_location + "/test_mkdir"
        mkdir_actual_dir = self.fileset_storage_location + "/test_mkdir"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )

        # it actually takes no effect.
        self.check_mkdir(mkdir_dir, mkdir_actual_dir, fs)

        # check whether it will automatically create the bucket if 'create_parents'
        # is set to True.
        new_bucket = self.azure_abs_container_name + "1"
        new_mkdir_actual_dir = mkdir_actual_dir.replace(
            self.azure_abs_container_name, new_bucket
        )
        self.fs.makedirs(new_mkdir_actual_dir)
        self.assertFalse(self.fs.exists(mkdir_actual_dir))
