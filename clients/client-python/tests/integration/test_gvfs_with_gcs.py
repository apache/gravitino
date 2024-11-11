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


@unittest.skip("This test require GCS service account key file")
class TestGvfsWithGCS(TestGvfsWithHDFS):
    # Before running this test, please set the make sure gcp-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/hadoop/libs/ directory
    key_file = "your_key_file.json"
    bucket_name = "your_bucket_name"
    metalake_name: str = "TestGvfsWithGCS_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_GCS_SERVICE_KEY_FILE}": self.key_file
        }

    def tearDown(self):
        self.options = {}

    @classmethod
    def setUpClass(cls):
        cls._get_gravitino_home()

        cls.hadoop_conf_path = f"{cls.gravitino_home}/catalogs/hadoop/conf/hadoop.conf"
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
                "filesystem-providers": "gcs",
                "gcs-service-account-file": cls.key_file,
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

        cls.fs = GCSFileSystem(token=cls.key_file)

    # Object storage like GCS does not support making directory and can only create
    # objects under the bucket. So we need to skip the test for GCS.
    def check_mkdir(self, gvfs_dir, actual_dir, gvfs_instance):
        # GCS will not create a directory, so the directory will not exist.
        self.fs.mkdir(actual_dir)
        self.assertFalse(self.fs.exists(actual_dir))
        self.assertFalse(gvfs_instance.exists(gvfs_dir))

    # Object storage like GCS does not support making directory and can only create
    # objects under the bucket. So we need to skip the test for GCS.
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
        # GCP only supports getting the `object` modify time, so the modified time will be None
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
        # fs.rm(rm_dir)

        # fs.rm(rm_dir, recursive=False) will delete the directory and the file
        # directly under the directory, so we comment the following code.
        # test delete dir with recursive = true
        # fs.rm(rm_dir, recursive=True)
        # self.assertFalse(fs.exists(rm_dir))

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

        # test delete file, GCS will remove the file directly.
        fs.rmdir(rmdir_file)

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
        self.assertFalse(fs.exists(mkdir_dir))
        self.assertFalse(self.fs.exists("gs://" + new_bucket))

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

        self.assertFalse(self.fs.exists(mkdir_actual_dir))
        self.assertFalse(fs.exists(mkdir_dir))
        self.assertFalse(self.fs.exists("gs://" + new_bucket))
