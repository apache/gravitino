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


from ossfs import OSSFileSystem

from gravitino import (
    GravitinoClient,
    Catalog,
    Fileset,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from tests.integration.test_gvfs_with_oss import TestGvfsWithOSS

logger = logging.getLogger(__name__)


def oss_with_credential_is_configured():
    return all(
        [
            os.environ.get("OSS_STS_ACCESS_KEY_ID") is not None,
            os.environ.get("OSS_STS_SECRET_ACCESS_KEY") is not None,
            os.environ.get("OSS_STS_ENDPOINT") is not None,
            os.environ.get("OSS_STS_BUCKET_NAME") is not None,
            os.environ.get("OSS_STS_REGION") is not None,
            os.environ.get("OSS_STS_ROLE_ARN") is not None,
        ]
    )


@unittest.skipUnless(
    oss_with_credential_is_configured(), "OSS with crednetial is not configured."
)
class TestGvfsWithOSSCredential(TestGvfsWithOSS):
    # Before running this test, please set the make sure aliyun-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/hadoop/libs/ directory
    oss_access_key = os.environ.get("OSS_STS_ACCESS_KEY_ID")
    oss_secret_key = os.environ.get("OSS_STS_SECRET_ACCESS_KEY")
    oss_endpoint = os.environ.get("OSS_STS_ENDPOINT")
    bucket_name = os.environ.get("OSS_STS_BUCKET_NAME")
    oss_sts_region = os.environ.get("OSS_STS_REGION")
    oss_sts_role_arn = os.environ.get("OSS_STS_ROLE_ARN")

    metalake_name: str = "TestGvfsWithOSSCredential_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_OSS_ACCESS_KEY}": self.oss_access_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_OSS_SECRET_KEY}": self.oss_secret_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_OSS_ENDPOINT}": self.oss_endpoint,
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
                "filesystem-providers": "oss",
                "oss-access-key-id": cls.oss_access_key,
                "oss-secret-access-key": cls.oss_secret_key,
                "oss-endpoint": cls.oss_endpoint,
                "oss-region": cls.oss_sts_region,
                "oss-role-arn": cls.oss_sts_role_arn,
                "credential-providers": "oss-token",
            },
        )
        catalog.as_schemas().create_schema(
            schema_name=cls.schema_name, comment="", properties={}
        )

        cls.fileset_storage_location: str = (
            f"oss://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
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

        cls.fs = OSSFileSystem(
            key=cls.oss_access_key,
            secret=cls.oss_secret_key,
            endpoint=cls.oss_endpoint,
        )
