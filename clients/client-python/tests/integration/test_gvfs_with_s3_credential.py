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

from s3fs import S3FileSystem

from gravitino import (
    gvfs,
    GravitinoClient,
    Catalog,
    Fileset,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from tests.integration.test_gvfs_with_s3 import TestGvfsWithS3

logger = logging.getLogger(__name__)


def s3_with_credential_is_configured():
    return all(
        [
            os.environ.get("S3_STS_ACCESS_KEY_ID") is not None,
            os.environ.get("S3_STS_SECRET_ACCESS_KEY") is not None,
            os.environ.get("S3_STS_ENDPOINT") is not None,
            os.environ.get("S3_STS_BUCKET_NAME") is not None,
            os.environ.get("S3_STS_REGION") is not None,
            os.environ.get("S3_STS_ROLE_ARN") is not None,
        ]
    )


@unittest.skipUnless(s3_with_credential_is_configured(), "S3 is not configured.")
class TestGvfsWithS3Credential(TestGvfsWithS3):
    # Before running this test, please set the make sure aws-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/fileset/libs/ directory
    s3_access_key = os.environ.get("S3_STS_ACCESS_KEY_ID")
    s3_secret_key = os.environ.get("S3_STS_SECRET_ACCESS_KEY")
    s3_endpoint = os.environ.get("S3_STS_ENDPOINT")
    bucket_name = os.environ.get("S3_STS_BUCKET_NAME")
    s3_sts_region = os.environ.get("S3_STS_REGION")
    s3_role_arn = os.environ.get("S3_STS_ROLE_ARN")

    metalake_name: str = "TestGvfsWithS3Credential_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_S3_ACCESS_KEY}": self.s3_access_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_S3_SECRET_KEY}": self.s3_secret_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_S3_ENDPOINT}": self.s3_endpoint,
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
                "filesystem-providers": "s3",
                "s3-access-key-id": cls.s3_access_key,
                "s3-secret-access-key": cls.s3_secret_key,
                "s3-endpoint": cls.s3_endpoint,
                "s3-region": cls.s3_sts_region,
                "s3-role-arn": cls.s3_role_arn,
                "credential-providers": "s3-token",
            },
        )
        catalog.as_schemas().create_schema(
            schema_name=cls.schema_name, comment="", properties={}
        )

        cls.fileset_storage_location: str = (
            f"s3a://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
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
            f"s3a://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/"
            f"{cls.multiple_locations_fileset_name}"
        )
        cls.multiple_locations_fileset_storage_location1: str = (
            f"s3a://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/"
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

        cls.fs = S3FileSystem(
            key=cls.s3_access_key,
            secret=cls.s3_secret_key,
            endpoint_url=cls.s3_endpoint,
        )

    # The following tests are copied from tests/integration/test_gvfs_with_s3.py, with some modifications as
    # `mkdir` and `makedirs` have different behavior in the S3, other cloud storage like GCS, ABS, and OSS.
    # are similar.
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

        fs.mkdir(mkdir_dir, create_parents=True)
        self.assertFalse(fs.exists(mkdir_dir))

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
