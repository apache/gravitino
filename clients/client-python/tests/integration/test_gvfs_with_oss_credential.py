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
from gravitino.filesystem import gvfs
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
    # copy to the $GRAVITINO_HOME/catalogs/fileset/libs/ directory
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

        cls.multiple_locations_fileset_storage_location: str = (
            f"oss://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/"
            f"{cls.multiple_locations_fileset_name}"
        )
        cls.multiple_locations_fileset_storage_location1: str = (
            f"oss://{cls.bucket_name}/{cls.catalog_name}/{cls.schema_name}/"
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

        cls.fs = OSSFileSystem(
            key=cls.oss_access_key,
            secret=cls.oss_secret_key,
            endpoint=cls.oss_endpoint,
        )

    def test_cat_file(self):
        cat_dir = self.fileset_gvfs_location + "/test_cat"
        cat_actual_dir = self.fileset_storage_location + "/test_cat"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )

        self.check_mkdir(cat_dir, cat_actual_dir, fs)

        cat_file = self.fileset_gvfs_location + "/test_cat/test.file"
        cat_actual_file = self.fileset_storage_location + "/test_cat/test.file"
        self.fs.touch(cat_actual_file)
        self.assertTrue(self.fs.exists(cat_actual_file))
        self.assertTrue(fs.exists(cat_file))

        # test open and write file
        with fs.open(cat_file, mode="wb") as f:
            f.write(b"test_cat_file")
        self.assertTrue(fs.info(cat_file)["size"] > 0)

        # test cat file
        content = fs.cat_file(cat_file)
        self.assertEqual(b"test_cat_file", content)

    @unittest.skip(
        "Skip this test case because fs.ls(dir) using credential is always empty"
    )
    def test_ls(self):
        ls_dir = self.fileset_gvfs_location + "/test_ls"
        ls_actual_dir = self.fileset_storage_location + "/test_ls"

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )

        self.check_mkdir(ls_dir, ls_actual_dir, fs)

        ls_file = self.fileset_gvfs_location + "/test_ls/test.file"
        ls_actual_file = self.fileset_storage_location + "/test_ls/test.file"
        self.fs.touch(ls_actual_file)
        self.assertTrue(self.fs.exists(ls_actual_file))

        # test detail = false
        file_list_without_detail = fs.ls(ls_dir, detail=False)
        self.assertEqual(1, len(file_list_without_detail))
        self.assertEqual(file_list_without_detail[0], ls_file[len("gvfs://") :])

        # test detail = true
        file_list_with_detail = fs.ls(ls_dir, detail=True)
        self.assertEqual(1, len(file_list_with_detail))
        self.assertEqual(file_list_with_detail[0]["name"], ls_file[len("gvfs://") :])

    @unittest.skip(
        "Skip this test case because fs.info(info_file) using credential is always None"
    )
    def test_info(self):
        info_dir = self.fileset_gvfs_location + "/test_info"
        info_actual_dir = self.fileset_storage_location + "/test_info"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )

        self.check_mkdir(info_dir, info_actual_dir, fs)

        info_file = self.fileset_gvfs_location + "/test_info/test.file"
        info_actual_file = self.fileset_storage_location + "/test_info/test.file"
        self.fs.touch(info_actual_file)
        self.assertTrue(self.fs.exists(info_actual_file))

        ## OSS info has different behavior than S3 info. For OSS info, the name of the
        ## directory will have a trailing slash if it's a directory and the path
        # does not end with a slash, while S3 info will not have a trailing
        # slash if it's a directory.

        # >> > oss.info('bucket-xiaoyu/lisi')
        # {'name': 'bucket-xiaoyu/lisi/', 'type': 'directory',
        # 'size': 0, 'Size': 0, 'Key': 'bucket-xiaoyu/lisi/'}
        # >> > oss.info('bucket-xiaoyu/lisi/')
        # {'name': 'bucket-xiaoyu/lisi', 'size': 0,
        # 'type': 'directory', 'Size': 0,
        # 'Key': 'bucket-xiaoyu/lisi'

        # >> > s3.info('paimon-bucket/lisi');
        # {'name': 'paimon-bucket/lisi', 'type': 'directory', 'size': 0,
        # 'StorageClass': 'DIRECTORY'}
        # >> > s3.info('paimon-bucket/lisi/');
        # {'name': 'paimon-bucket/lisi', 'type': 'directory', 'size': 0,
        # 'StorageClass': 'DIRECTORY'}

        dir_info = fs.info(info_dir)
        self.assertEqual(dir_info["name"][:-1], info_dir[len("gvfs://") :])

        file_info = fs.info(info_file)
        self.assertEqual(file_info["name"], info_file[len("gvfs://") :])
