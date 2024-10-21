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

from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow.fs import S3FileSystem

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


@unittest.skip("This test require S3 service account")
class TestGvfsWithS3(TestGvfsWithHDFS):
    # Before running this test, please set the make sure aws-bundle-x.jar has been
    # copy to the $GRAVITINO_HOME/catalogs/hadoop/libs/ directory
    s3_access_key = "your_access_key"
    s3_secret_key = "your_secret_key"
    s3_endpoint = "your_endpoint"
    bucket_name = "your_bucket_name"

    metalake_name: str = "TestGvfsWithS3_metalake" + str(randint(1, 10000))

    def setUp(self):
        self.options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_BY_PASS_S3}{GVFSConfig.GVFS_FILESYSTEM_S3_ACCESS_KEY}": self.s3_access_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_BY_PASS_S3}{GVFSConfig.GVFS_FILESYSTEM_S3_SECRET_KEY}": self.s3_secret_key,
            f"{GVFSConfig.GVFS_FILESYSTEM_BY_PASS_S3}{GVFSConfig.GVFS_FILESYSTEM_S3_ENDPOINT}": self.s3_endpoint,
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
                "filesystem-providers": "s3",
                "gravitino.bypass.fs.s3a.access.key": cls.s3_access_key,
                "gravitino.bypass.fs.s3a.secret.key": cls.s3_secret_key,
                "gravitino.bypass.fs.s3a.endpoint": cls.s3_endpoint,
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

        arrow_s3_fs = S3FileSystem(
            access_key=cls.s3_access_key,
            secret_key=cls.s3_secret_key,
            endpoint_override=cls.s3_endpoint,
        )
        cls.fs = ArrowFSWrapper(arrow_s3_fs)

    def test_modified(self):
        modified_dir = self.fileset_gvfs_location + "/test_modified"
        modified_actual_dir = self.fileset_storage_location + "/test_modified"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=self.options,
            **self.conf,
        )
        self.fs.mkdir(modified_actual_dir)
        self.assertTrue(self.fs.exists(modified_actual_dir))
        self.assertTrue(fs.exists(modified_dir))

        self.assertIsNotNone(fs.modified(modified_dir))

        # create a file under the dir 'modified_dir'.
        file_path = modified_dir + "/test.txt"
        fs.touch(file_path)
        self.assertTrue(fs.exists(file_path))
        self.assertIsNotNone(fs.modified(file_path))

    # @unittest.skip(
    #     "This test will fail for https://github.com/apache/arrow/issues/44438"
    # )
    # def test_pandas(self):
    #     pass
    #
    # @unittest.skip(
    #     "This test will fail for https://github.com/apache/arrow/issues/44438"
    # )
    # def test_pyarrow(self):
    #     pass
