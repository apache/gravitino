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


def s3_is_configured():
    return all(
        [
            os.environ.get("S3_ACCESS_KEY_ID") is not None,
            os.environ.get("S3_SECRET_ACCESS_KEY") is not None,
            os.environ.get("S3_ENDPOINT") is not None,
            os.environ.get("S3_BUCKET_NAME") is not None,
        ]
    )


@unittest.skipUnless(s3_is_configured(), "S3 is not configured.")
class TestGvfsWithS3(TestGvfsWithHDFS):
    # Before running this test, please set the make sure aws-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/fileset/libs/ directory
    s3_access_key = os.environ.get("S3_ACCESS_KEY_ID")
    s3_secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")
    s3_endpoint = os.environ.get("S3_ENDPOINT")
    bucket_name = os.environ.get("S3_BUCKET_NAME")

    metalake_name: str = "TestGvfsWithS3_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_S3_ACCESS_KEY}": self.s3_access_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_S3_SECRET_KEY}": self.s3_secret_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_S3_ENDPOINT}": self.s3_endpoint,
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
            uri="http://localhost:8090",
            metalake_name=cls.metalake_name,
            client_config={"gravitino_client_request_timeout": 180},
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

    def check_mkdir(self, gvfs_dir, actual_dir, gvfs_instance):
        # S3 will not create a directory, so the directory will not exist.
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

    # pylint: disable=W0212
    def test_mkdir(self):
        mkdir_dir = self.fileset_gvfs_location + "/test_mkdir"
        mkdir_actual_dir = self.fileset_storage_location + "/test_mkdir"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            config_kwargs={"s3": {"addressing_style": "virtual"}},
            **self.conf,
        )

        if fs.operations._enable_fileset_metadata_cache:
            fs.operations._fileset_cache.clear()
            fs.operations._catalog_cache.clear()

        s3_fs = fs.operations._get_actual_filesystem(mkdir_dir, None)
        config_kwargs = s3_fs.config_kwargs
        self.assertEqual("virtual", config_kwargs.get("s3").get("addressing_style"))

        # it actually takes no effect.
        self.check_mkdir(mkdir_dir, mkdir_actual_dir, fs)

        # check whether it will automatically create the bucket if 'create_parents'
        # is set to True.
        new_bucket = self.bucket_name + "1"
        mkdir_dir = mkdir_dir.replace(self.bucket_name, new_bucket)
        fs.mkdir(mkdir_dir, create_parents=True)

        self.assertFalse(fs.exists(mkdir_dir))
        self.assertFalse(self.fs.exists("s3://" + new_bucket))

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
        with self.assertRaises(OSError):
            self.fs.exists(mkdir_actual_dir)

        self.assertFalse(fs.exists(mkdir_dir))
        self.assertFalse(self.fs.exists("s3://" + new_bucket))

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
