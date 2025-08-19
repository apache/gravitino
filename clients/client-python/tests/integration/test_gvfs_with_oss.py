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

from tests.integration.test_gvfs_with_hdfs import TestGvfsWithHDFS
from gravitino import (
    gvfs,
    GravitinoClient,
    Catalog,
    Fileset,
)
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.filesystem.gvfs_config import GVFSConfig


logger = logging.getLogger(__name__)


def oss_is_configured():
    return all(
        [
            os.environ.get("OSS_ACCESS_KEY_ID") is not None,
            os.environ.get("OSS_SECRET_ACCESS_KEY") is not None,
            os.environ.get("OSS_ENDPOINT") is not None,
            os.environ.get("OSS_BUCKET_NAME") is not None,
        ]
    )


@unittest.skipUnless(oss_is_configured(), "OSS is not configured.")
class TestGvfsWithOSS(TestGvfsWithHDFS):
    # Before running this test, please set the make sure aliyun-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/fileset/libs/ directory
    oss_access_key = os.environ.get("OSS_ACCESS_KEY_ID")
    oss_secret_key = os.environ.get("OSS_SECRET_ACCESS_KEY")
    oss_endpoint = os.environ.get("OSS_ENDPOINT")
    bucket_name = os.environ.get("OSS_BUCKET_NAME")

    metalake_name: str = "TestGvfsWithOSS_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_OSS_ACCESS_KEY}": self.oss_access_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_OSS_SECRET_KEY}": self.oss_secret_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_OSS_ENDPOINT}": self.oss_endpoint,
        }

    def tearDown(self):
        self.options = {}

    @classmethod
    def setUpClass(cls):
        cls._get_gravitino_home()

        cls.hadoop_conf_path = (
            f"{cls.gravitino_home}/catalogs/fileset/conf/fileset.conf"
        )
        # restart the server
        cls.restart_server()
        # create entity
        cls._init_test_entities()

    @classmethod
    def tearDownClass(cls):
        cls._clean_test_data()
        # reset server conf in case of other ITs like HDFS has changed it and fail
        # to reset it
        cls._reset_conf(cls.config, cls.hadoop_conf_path)
        # restart server
        cls.restart_server()

    # clear all config in the conf_path
    @classmethod
    def _reset_conf(cls, config, conf_path):
        logger.info("Reset %s.", conf_path)
        if not os.path.exists(conf_path):
            raise GravitinoRuntimeException(f"Conf file is not found at `{conf_path}`.")
        filtered_lines = []
        with open(conf_path, mode="r", encoding="utf-8") as file:
            origin_lines = file.readlines()

        for line in origin_lines:
            line = line.strip()
            if line.startswith("#"):
                # append annotations directly
                filtered_lines.append(line + "\n")

        with open(conf_path, mode="w", encoding="utf-8") as file:
            for line in filtered_lines:
                file.write(line)

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

    def check_mkdir(self, gvfs_dir, actual_dir, gvfs_instance):
        # OSS will not create a directory, so the directory will not exist.
        self.fs.mkdir(actual_dir)
        self.assertFalse(self.fs.exists(actual_dir))
        self.assertFalse(gvfs_instance.exists(gvfs_dir))

    def check_makedirs(self, gvfs_dir, actual_dir, gvfs_instance):
        self.fs.makedirs(actual_dir)
        self.assertFalse(self.fs.exists(actual_dir))
        self.assertFalse(gvfs_instance.exists(gvfs_dir))

    def test_modified(self):
        modified_dir = self.fileset_gvfs_location + "/test_modified"
        modified_actual_dir = self.fileset_storage_location + "/test_modified"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )

        self.check_mkdir(modified_dir, modified_actual_dir, fs)
        # S3 only supports getting the `object` modify time, so the modified time will be None
        # if it's a directory.
        # >>> gcs.mkdir('example_qazwsx/catalog/schema/fileset3')
        # >>> r = gcs.modified('example_qazwsx/catalog/schema/fileset3')
        # >>> print(r)
        # None
        # self.assertIsNone(fs.modified(modified_dir))

        # create a file under the dir 'modified_dir'.
        file_path = modified_dir + "/test.txt"
        fs.touch(file_path)
        self.assertTrue(fs.exists(file_path))
        self.assertIsNotNone(fs.modified(file_path))

    def test_rm(self):
        rm_dir = self.fileset_gvfs_location + "/test_rm"
        rm_actual_dir = self.fileset_storage_location + "/test_rm"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )
        self.check_mkdir(rm_dir, rm_actual_dir, fs)

        rm_file = self.fileset_gvfs_location + "/test_rm/test.file"
        rm_actual_file = self.fileset_storage_location + "/test_rm/test.file"
        fs.touch(rm_file)
        self.assertTrue(self.fs.exists(rm_actual_file))
        self.assertTrue(fs.exists(rm_file))

        # test delete file
        fs.rm(rm_file)
        self.assertFalse(fs.exists(rm_file))

        # test delete dir with recursive = false
        rm_new_file = self.fileset_gvfs_location + "/test_rm/test_new.file"
        rm_new_actual_file = self.fileset_storage_location + "/test_rm/test_new.file"
        self.fs.touch(rm_new_actual_file)
        self.assertTrue(self.fs.exists(rm_new_actual_file))
        self.assertTrue(fs.exists(rm_new_file))

    def test_rmdir(self):
        rmdir_dir = self.fileset_gvfs_location + "/test_rmdir"
        rmdir_actual_dir = self.fileset_storage_location + "/test_rmdir"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )
        self.check_mkdir(rmdir_dir, rmdir_actual_dir, fs)

        rmdir_file = self.fileset_gvfs_location + "/test_rmdir/test.file"
        rmdir_actual_file = self.fileset_storage_location + "/test_rmdir/test.file"
        self.fs.touch(rmdir_actual_file)
        self.assertTrue(self.fs.exists(rmdir_actual_file))
        self.assertTrue(fs.exists(rmdir_file))

        fs.rm_file(rmdir_file)

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

        with self.assertRaises(FileNotFoundError):
            self.fs.exists(mkdir_actual_dir)

        self.assertFalse(fs.exists(mkdir_dir))

        with self.assertRaises(FileNotFoundError):
            self.fs.exists("oss://" + new_bucket)

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
        new_bucket = self.bucket_name + "1"
        mkdir_dir = mkdir_dir.replace(self.bucket_name, new_bucket)
        mkdir_actual_dir = mkdir_actual_dir.replace(self.bucket_name, new_bucket)

        # it takes no effect.
        fs.makedirs(mkdir_dir)

        with self.assertRaises(FileNotFoundError):
            self.fs.exists(mkdir_actual_dir)

        self.assertFalse(fs.exists(mkdir_dir))
        with self.assertRaises(FileNotFoundError):
            self.fs.exists("oss://" + new_bucket)

    def test_rm_file(self):
        rm_file_dir = self.fileset_gvfs_location + "/test_rm_file"
        rm_file_actual_dir = self.fileset_storage_location + "/test_rm_file"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )
        self.check_mkdir(rm_file_dir, rm_file_actual_dir, fs)

        rm_file_file = self.fileset_gvfs_location + "/test_rm_file/test.file"
        rm_file_actual_file = self.fileset_storage_location + "/test_rm_file/test.file"
        self.fs.touch(rm_file_actual_file)
        self.assertTrue(self.fs.exists(rm_file_actual_file))
        self.assertTrue(fs.exists(rm_file_file))

        # test delete file
        fs.rm_file(rm_file_file)
        self.assertFalse(fs.exists(rm_file_file))

        # test delete dir
        fs.rm_file(rm_file_dir)

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
