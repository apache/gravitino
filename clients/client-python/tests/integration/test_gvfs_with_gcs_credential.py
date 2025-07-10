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

from gcsfs import GCSFileSystem

from gravitino import Catalog, Fileset, GravitinoClient
from gravitino.filesystem import gvfs
from gravitino.filesystem.gvfs_config import GVFSConfig
from tests.integration.test_gvfs_with_gcs import TestGvfsWithGCS

logger = logging.getLogger(__name__)


def gcs_with_credential_is_configured():
    return all(
        [
            os.environ.get("GCS_SERVICE_ACCOUNT_JSON_PATH_FOR_CREDENTIAL") is not None,
            os.environ.get("GCS_BUCKET_NAME_FOR_CREDENTIAL") is not None,
        ]
    )


@unittest.skipUnless(gcs_with_credential_is_configured(), "GCS is not configured.")
class TestGvfsWithGCSCredential(TestGvfsWithGCS):
    # Before running this test, please set the make sure gcp-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/fileset/libs/ directory
    key_file = os.environ.get("GCS_SERVICE_ACCOUNT_JSON_PATH_FOR_CREDENTIAL")
    bucket_name = os.environ.get("GCS_BUCKET_NAME_FOR_CREDENTIAL")
    metalake_name: str = "TestGvfsWithGCSCredential_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_GCS_SERVICE_KEY_FILE}": self.key_file,
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
                "filesystem-providers": "gcs",
                "gcs-credential-file-path": cls.key_file,
                "gcs-service-account-file": cls.key_file,
                "credential-providers": "gcs-token",
            },
        )
        catalog.as_schemas().create_schema(
            schema_name=cls.schema_name, comment="", properties={}
        )

        cls.fileset_storage_location: str = (
            f"gs://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
        )
        cls.fileset_gvfs_location = (
            f"gvfs://fileset/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
        )
        catalog.as_fileset_catalog().create_fileset(
            ident=cls.fileset_ident,
            fileset_type=Fileset.Type.MANAGED,
            comment=cls.fileset_comment,
            storage_location=cls.fileset_storage_location,
            properties=cls.fileset_properties,
        )

        cls.multiple_locations_fileset_storage_location: str = (
            f"gs://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/"
            f"{cls.multiple_locations_fileset_name}"
        )
        cls.multiple_locations_fileset_storage_location1: str = (
            f"gs://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/"
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

        cls.fs = GCSFileSystem(token=cls.key_file)

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
        new_bucket = self.bucket_name + "1"
        mkdir_dir = mkdir_dir.replace(self.bucket_name, new_bucket)
        mkdir_actual_dir = mkdir_actual_dir.replace(self.bucket_name, new_bucket)

        fs.mkdir(mkdir_dir, create_parents=True)
        self.assertFalse(self.fs.exists(mkdir_actual_dir))
