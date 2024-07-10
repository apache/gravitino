"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

# pylint: disable=protected-access

import base64
import logging
import os
import platform
import unittest
from random import randint
from typing import Dict

import pandas
import pyarrow as pa
import pyarrow.dataset as dt
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from llama_index.core import SimpleDirectoryReader
from pyarrow.fs import HadoopFileSystem
from gravitino import (
    gvfs,
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
    Fileset,
)
from gravitino.auth.auth_constants import AuthConstants
from gravitino.exceptions.base import GravitinoRuntimeException
from tests.integration.integration_test_env import IntegrationTestEnv
from tests.integration.hdfs_container import HDFSContainer
from tests.integration.base_hadoop_env import BaseHadoopEnvironment

logger = logging.getLogger(__name__)

DOCKER_TEST = os.environ.get("DOCKER_TEST")


#  The Hadoop distribution package does not have native hdfs libraries for macOS / Windows systems
#  (`libhdfs.dylib` for macOS and `libhdfs.dll` for Windows), so the integration tests cannot be run
#  on these two systems at present.
@unittest.skipIf(
    DOCKER_TEST == "false" or platform.system() != "Linux",
    "Skipping tests on non-Linux systems or when DOCKER_TEST=false",
)
class TestGvfsWithHDFS(IntegrationTestEnv):
    hdfs_container: HDFSContainer = None
    config: Dict = None
    metalake_name: str = "TestGvfsWithHDFS_metalake" + str(randint(1, 10000))
    catalog_name: str = "test_gvfs_catalog" + str(randint(1, 10000))
    catalog_provider: str = "hadoop"

    schema_name: str = "test_gvfs_schema"

    fileset_name: str = "test_gvfs_fileset"
    fileset_comment: str = "fileset_comment"
    fileset_storage_location: str = ""
    fileset_properties_key1: str = "fileset_properties_key1"
    fileset_properties_value1: str = "fileset_properties_value1"
    fileset_properties_key2: str = "fileset_properties_key2"
    fileset_properties_value2: str = "fileset_properties_value2"
    fileset_properties: Dict[str, str] = {
        fileset_properties_key1: fileset_properties_value1,
        fileset_properties_key2: fileset_properties_value2,
    }

    schema_ident: NameIdentifier = NameIdentifier.of(
        metalake_name, catalog_name, schema_name
    )
    fileset_ident: NameIdentifier = NameIdentifier.of(schema_name, fileset_name)

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    gravitino_client: GravitinoClient = None

    @classmethod
    def setUpClass(cls):
        cls.hdfs_container = HDFSContainer()
        hdfs_container_ip = cls.hdfs_container.get_ip()
        # init hadoop env
        BaseHadoopEnvironment.init_hadoop_env()
        cls.config = {
            "gravitino.bypass.fs.defaultFS": f"hdfs://{hdfs_container_ip}:9000"
        }
        # append the hadoop conf to server
        cls._append_catalog_hadoop_conf(cls.config)
        # restart the server
        cls.restart_server()
        # create entity
        cls._init_test_entities()

    @classmethod
    def tearDownClass(cls):
        try:
            cls._clean_test_data()
            # reset server conf
            cls._reset_catalog_hadoop_conf(cls.config)
            # restart server
            cls.restart_server()
            # clear hadoop env
            BaseHadoopEnvironment.clear_hadoop_env()
        finally:
            # close hdfs container
            cls.hdfs_container.close()

    @classmethod
    def _init_test_entities(cls):
        cls.gravitino_admin_client.create_metalake(
            name=cls.metalake_name, comment="", properties={}
        )
        cls.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls.metalake_name
        )
        catalog = cls.gravitino_client.create_catalog(
            name=cls.catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider=cls.catalog_provider,
            comment="",
            properties={},
        )
        catalog.as_schemas().create_schema(
            schema_name=cls.schema_name, comment="", properties={}
        )

        cls.fileset_storage_location: str = (
            f"hdfs://{cls.hdfs_container.get_ip()}:9000/{cls.catalog_name}/{cls.schema_name}/{cls.fileset_name}"
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
        arrow_hadoop_fs = HadoopFileSystem(host=cls.hdfs_container.get_ip(), port=9000)
        cls.hdfs = ArrowFSWrapper(arrow_hadoop_fs)
        cls.conf: Dict = {"fs.defaultFS": f"hdfs://{cls.hdfs_container.get_ip()}:9000/"}

    @classmethod
    def _clean_test_data(cls):
        try:
            cls.gravitino_client = GravitinoClient(
                uri="http://localhost:8090", metalake_name=cls.metalake_name
            )
            catalog = cls.gravitino_client.load_catalog(name=cls.catalog_name)
            logger.info(
                "Drop fileset %s[%s]",
                cls.fileset_ident,
                catalog.as_fileset_catalog().drop_fileset(ident=cls.fileset_ident),
            )
            logger.info(
                "Drop schema %s[%s]",
                cls.schema_ident,
                catalog.as_schemas().drop_schema(
                    schema_name=cls.schema_name, cascade=True
                ),
            )
            logger.info(
                "Drop catalog %s[%s]",
                cls.catalog_name,
                cls.gravitino_client.drop_catalog(name=cls.catalog_name),
            )
            logger.info(
                "Drop metalake %s[%s]",
                cls.metalake_name,
                cls.gravitino_admin_client.drop_metalake(cls.metalake_name),
            )
        except Exception as e:
            logger.error("Clean test data failed: %s", e)

    def test_simple_auth(self):
        options = {"auth_type": "simple"}
        current_user = (
            None if os.environ.get("user.name") is None else os.environ["user.name"]
        )
        user = "test_gvfs"
        os.environ["user.name"] = user
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            options=options,
        )
        token = fs._client._rest_client.auth_data_provider.get_token_data()
        token_string = base64.b64decode(
            token.decode("utf-8")[len(AuthConstants.AUTHORIZATION_BASIC_HEADER) :]
        ).decode("utf-8")
        self.assertEqual(f"{user}:dummy", token_string)
        if current_user is not None:
            os.environ["user.name"] = current_user

    def test_ls(self):
        ls_dir = self.fileset_gvfs_location + "/test_ls"
        ls_actual_dir = self.fileset_storage_location + "/test_ls"

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(ls_actual_dir)
        self.assertTrue(self.hdfs.exists(ls_actual_dir))

        ls_file = self.fileset_gvfs_location + "/test_ls/test.file"
        ls_actual_file = self.fileset_storage_location + "/test_ls/test.file"
        self.hdfs.touch(ls_actual_file)
        self.assertTrue(self.hdfs.exists(ls_actual_file))

        # test detail = false
        file_list_without_detail = fs.ls(ls_dir, detail=False)
        self.assertEqual(1, len(file_list_without_detail))
        self.assertEqual(file_list_without_detail[0], ls_file[len("gvfs://") :])

        # test detail = true
        file_list_with_detail = fs.ls(ls_dir, detail=True)
        self.assertEqual(1, len(file_list_with_detail))
        self.assertEqual(file_list_with_detail[0]["name"], ls_file[len("gvfs://") :])

    def test_info(self):
        info_dir = self.fileset_gvfs_location + "/test_info"
        info_actual_dir = self.fileset_storage_location + "/test_info"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(info_actual_dir)
        self.assertTrue(self.hdfs.exists(info_actual_dir))

        info_file = self.fileset_gvfs_location + "/test_info/test.file"
        info_actual_file = self.fileset_storage_location + "/test_info/test.file"
        self.hdfs.touch(info_actual_file)
        self.assertTrue(self.hdfs.exists(info_actual_file))

        dir_info = fs.info(info_dir)
        self.assertEqual(dir_info["name"], info_dir[len("gvfs://") :])

        file_info = fs.info(info_file)
        self.assertEqual(file_info["name"], info_file[len("gvfs://") :])

    def test_exist(self):
        exist_dir = self.fileset_gvfs_location + "/test_exist"
        exist_actual_dir = self.fileset_storage_location + "/test_exist"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(exist_actual_dir)
        self.assertTrue(self.hdfs.exists(exist_actual_dir))
        self.assertTrue(fs.exists(exist_dir))

        exist_file = self.fileset_gvfs_location + "/test_exist/test.file"
        exist_actual_file = self.fileset_storage_location + "/test_exist/test.file"
        self.hdfs.touch(exist_actual_file)
        self.assertTrue(self.hdfs.exists(exist_actual_file))
        self.assertTrue(fs.exists(exist_file))

    def test_cp_file(self):
        cp_file_dir = self.fileset_gvfs_location + "/test_cp_file"
        cp_file_actual_dir = self.fileset_storage_location + "/test_cp_file"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(cp_file_actual_dir)
        self.assertTrue(self.hdfs.exists(cp_file_actual_dir))
        self.assertTrue(fs.exists(cp_file_dir))

        cp_file_file = self.fileset_gvfs_location + "/test_cp_file/test.file"
        cp_file_actual_file = self.fileset_storage_location + "/test_cp_file/test.file"
        self.hdfs.touch(cp_file_actual_file)
        self.assertTrue(self.hdfs.exists(cp_file_actual_file))
        self.assertTrue(fs.exists(cp_file_file))

        with self.hdfs.open(cp_file_actual_file, "wb") as f:
            f.write(b"test_file_1")

        cp_file_new_file = self.fileset_gvfs_location + "/test_cp_file/test_cp.file"
        cp_file_new_actual_file = (
            self.fileset_storage_location + "/test_cp_file/test_cp.file"
        )
        fs.cp_file(cp_file_file, cp_file_new_file)
        self.assertTrue(fs.exists(cp_file_new_file))

        with self.hdfs.open(cp_file_new_actual_file, "rb") as f:
            result = f.read()
        self.assertEqual(b"test_file_1", result)

    def test_mv(self):
        mv_dir = self.fileset_gvfs_location + "/test_mv"
        mv_actual_dir = self.fileset_storage_location + "/test_mv"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(mv_actual_dir)
        self.assertTrue(self.hdfs.exists(mv_actual_dir))
        self.assertTrue(fs.exists(mv_dir))

        mv_new_dir = self.fileset_gvfs_location + "/test_mv_new"
        mv_new_actual_dir = self.fileset_storage_location + "/test_mv_new"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(mv_new_actual_dir)
        self.assertTrue(self.hdfs.exists(mv_new_actual_dir))
        self.assertTrue(fs.exists(mv_new_dir))

        mv_file = self.fileset_gvfs_location + "/test_mv/test.file"
        mv_actual_file = self.fileset_storage_location + "/test_mv/test.file"
        self.hdfs.touch(mv_actual_file)
        self.assertTrue(self.hdfs.exists(mv_actual_file))
        self.assertTrue(fs.exists(mv_file))

        mv_new_file = self.fileset_gvfs_location + "/test_mv_new/test_new.file"
        mv_new_actual_file = (
            self.fileset_storage_location + "/test_mv_new/test_new.file"
        )

        fs.mv(mv_file, mv_new_file)
        self.assertTrue(fs.exists(mv_new_file))
        self.assertTrue(self.hdfs.exists(mv_new_actual_file))

    def test_rm(self):
        rm_dir = self.fileset_gvfs_location + "/test_rm"
        rm_actual_dir = self.fileset_storage_location + "/test_rm"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(rm_actual_dir)
        self.assertTrue(self.hdfs.exists(rm_actual_dir))
        self.assertTrue(fs.exists(rm_dir))

        rm_file = self.fileset_gvfs_location + "/test_rm/test.file"
        rm_actual_file = self.fileset_storage_location + "/test_rm/test.file"
        self.hdfs.touch(rm_file)
        self.assertTrue(self.hdfs.exists(rm_actual_file))
        self.assertTrue(fs.exists(rm_file))

        # test delete file
        fs.rm(rm_file)
        self.assertFalse(fs.exists(rm_file))

        # test delete dir with recursive = false
        rm_new_file = self.fileset_gvfs_location + "/test_rm/test_new.file"
        rm_new_actual_file = self.fileset_storage_location + "/test_rm/test_new.file"
        self.hdfs.touch(rm_new_actual_file)
        self.assertTrue(self.hdfs.exists(rm_new_actual_file))
        self.assertTrue(fs.exists(rm_new_file))
        with self.assertRaises(ValueError):
            fs.rm(rm_dir, recursive=False)

        # test delete dir with recursive = true
        fs.rm(rm_dir, recursive=True)
        self.assertFalse(fs.exists(rm_dir))

    def test_rm_file(self):
        rm_file_dir = self.fileset_gvfs_location + "/test_rm_file"
        rm_file_actual_dir = self.fileset_storage_location + "/test_rm_file"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(rm_file_actual_dir)
        self.assertTrue(self.hdfs.exists(rm_file_actual_dir))
        self.assertTrue(fs.exists(rm_file_dir))

        rm_file_file = self.fileset_gvfs_location + "/test_rm_file/test.file"
        rm_file_actual_file = self.fileset_storage_location + "/test_rm_file/test.file"
        self.hdfs.touch(rm_file_actual_file)
        self.assertTrue(self.hdfs.exists(rm_file_actual_file))
        self.assertTrue(fs.exists(rm_file_file))

        # test delete file
        fs.rm_file(rm_file_file)
        self.assertFalse(fs.exists(rm_file_file))

        # test delete dir
        with self.assertRaises(OSError):
            fs.rm_file(rm_file_dir)

    def test_rmdir(self):
        rmdir_dir = self.fileset_gvfs_location + "/test_rmdir"
        rmdir_actual_dir = self.fileset_storage_location + "/test_rmdir"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(rmdir_actual_dir)
        self.assertTrue(self.hdfs.exists(rmdir_actual_dir))
        self.assertTrue(fs.exists(rmdir_dir))

        rmdir_file = self.fileset_gvfs_location + "/test_rmdir/test.file"
        rmdir_actual_file = self.fileset_storage_location + "/test_rmdir/test.file"
        self.hdfs.touch(rmdir_actual_file)
        self.assertTrue(self.hdfs.exists(rmdir_actual_file))
        self.assertTrue(fs.exists(rmdir_file))

        # test delete file
        with self.assertRaises(OSError):
            fs.rmdir(rmdir_file)

        # test delete dir
        fs.rmdir(rmdir_dir)
        self.assertFalse(fs.exists(rmdir_dir))

    def test_open(self):
        open_dir = self.fileset_gvfs_location + "/test_open"
        open_actual_dir = self.fileset_storage_location + "/test_open"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(open_actual_dir)
        self.assertTrue(self.hdfs.exists(open_actual_dir))
        self.assertTrue(fs.exists(open_dir))

        open_file = self.fileset_gvfs_location + "/test_open/test.file"
        open_actual_file = self.fileset_storage_location + "/test_open/test.file"
        self.hdfs.touch(open_actual_file)
        self.assertTrue(self.hdfs.exists(open_actual_file))
        self.assertTrue(fs.exists(open_file))

        # test open and write file
        with fs.open(open_file, mode="wb") as f:
            f.write(b"test_open_write")
        self.assertTrue(fs.info(open_file)["size"] > 0)

        # test open and read file
        with fs.open(open_file, mode="rb") as f:
            self.assertEqual(b"test_open_write", f.read())

    def test_mkdir(self):
        mkdir_dir = self.fileset_gvfs_location + "/test_mkdir"
        mkdir_actual_dir = self.fileset_storage_location + "/test_mkdir"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        fs.mkdir(mkdir_dir)
        self.assertTrue(fs.exists(mkdir_dir))
        self.assertTrue(self.hdfs.exists(mkdir_actual_dir))

        # test mkdir dir with create_parents = false
        parent_not_exist_virtual_path = mkdir_dir + "/not_exist/sub_dir"
        self.assertFalse(fs.exists(parent_not_exist_virtual_path))

        with self.assertRaises(OSError):
            fs.mkdir(parent_not_exist_virtual_path, create_parents=False)

        # test mkdir dir with create_parents = true
        parent_not_exist_virtual_path2 = mkdir_dir + "/not_exist/sub_dir"
        self.assertFalse(fs.exists(parent_not_exist_virtual_path2))

        fs.mkdir(parent_not_exist_virtual_path2, create_parents=True)
        self.assertTrue(fs.exists(parent_not_exist_virtual_path2))

    def test_makedirs(self):
        makedirs_dir = self.fileset_gvfs_location + "/test_makedirs"
        makedirs_actual_dir = self.fileset_storage_location + "/test_makedirs"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        fs.makedirs(makedirs_dir)
        self.assertTrue(fs.exists(makedirs_dir))
        self.assertTrue(self.hdfs.exists(makedirs_actual_dir))

        # test mkdir dir not exist
        parent_not_exist_virtual_path = makedirs_dir + "/not_exist/sub_dir"
        self.assertFalse(fs.exists(parent_not_exist_virtual_path))
        fs.makedirs(parent_not_exist_virtual_path)
        self.assertTrue(fs.exists(parent_not_exist_virtual_path))

    def test_created(self):
        created_dir = self.fileset_gvfs_location + "/test_created"
        created_actual_dir = self.fileset_storage_location + "/test_created"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(created_actual_dir)
        self.assertTrue(self.hdfs.exists(created_actual_dir))
        self.assertTrue(fs.exists(created_dir))

        with self.assertRaises(GravitinoRuntimeException):
            fs.created(created_dir)

    def test_modified(self):
        modified_dir = self.fileset_gvfs_location + "/test_modified"
        modified_actual_dir = self.fileset_storage_location + "/test_modified"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(modified_actual_dir)
        self.assertTrue(self.hdfs.exists(modified_actual_dir))
        self.assertTrue(fs.exists(modified_dir))

        # test mkdir dir which exists
        self.assertIsNotNone(fs.modified(modified_dir))

    def test_cat_file(self):
        cat_dir = self.fileset_gvfs_location + "/test_cat"
        cat_actual_dir = self.fileset_storage_location + "/test_cat"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(cat_actual_dir)
        self.assertTrue(self.hdfs.exists(cat_actual_dir))
        self.assertTrue(fs.exists(cat_dir))

        cat_file = self.fileset_gvfs_location + "/test_cat/test.file"
        cat_actual_file = self.fileset_storage_location + "/test_cat/test.file"
        self.hdfs.touch(cat_actual_file)
        self.assertTrue(self.hdfs.exists(cat_actual_file))
        self.assertTrue(fs.exists(cat_file))

        # test open and write file
        with fs.open(cat_file, mode="wb") as f:
            f.write(b"test_cat_file")
        self.assertTrue(fs.info(cat_file)["size"] > 0)

        # test cat file
        content = fs.cat_file(cat_file)
        self.assertEqual(b"test_cat_file", content)

    def test_get_file(self):
        get_dir = self.fileset_gvfs_location + "/test_get"
        get_actual_dir = self.fileset_storage_location + "/test_get"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(get_actual_dir)
        self.assertTrue(self.hdfs.exists(get_actual_dir))
        self.assertTrue(fs.exists(get_dir))

        get_file = self.fileset_gvfs_location + "/test_get/test.file"
        get_actual_file = self.fileset_storage_location + "/test_get/test.file"
        self.hdfs.touch(get_actual_file)
        self.assertTrue(self.hdfs.exists(get_actual_file))
        self.assertTrue(fs.exists(get_file))

        # test open and write file
        with fs.open(get_file, mode="wb") as f:
            f.write(b"test_get_file")
        self.assertTrue(fs.info(get_file)["size"] > 0)

        # test get file
        local_fs = LocalFileSystem()
        local_dir = "/tmp/test_gvfs_local_file_" + str(randint(1, 10000))
        local_fs.makedirs(local_dir)
        local_path = local_dir + "/get_file.txt"
        local_fs.touch(local_path)
        self.assertTrue(local_fs.exists(local_path))
        fs.get_file(get_file, local_path)
        self.assertEqual(b"test_get_file", local_fs.cat_file(local_path))
        local_fs.rm(local_dir, recursive=True)

        # test get a file to a remote file
        remote_path = self.fileset_gvfs_location + "/test_file_2.par"
        with self.assertRaises(GravitinoRuntimeException):
            fs.get_file(get_file, remote_path)

    def test_pandas(self):
        pands_dir = self.fileset_gvfs_location + "/test_pandas"
        pands_actual_dir = self.fileset_storage_location + "/test_pandas"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(pands_actual_dir)
        self.assertTrue(self.hdfs.exists(pands_actual_dir))
        self.assertTrue(fs.exists(pands_dir))

        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
        # to parquet
        parquet_file = self.fileset_gvfs_location + "/test_pandas/test.parquet"
        parquet_actual_file = (
            self.fileset_storage_location + "/test_pandas/test.parquet"
        )
        data.to_parquet(parquet_file, filesystem=fs)
        self.assertTrue(fs.exists(parquet_file))
        self.assertTrue(self.hdfs.exists(parquet_actual_file))

        # read parquet
        ds1 = pandas.read_parquet(path=parquet_file, filesystem=fs)
        self.assertTrue(data.equals(ds1))
        storage_options = {
            "server_uri": "http://localhost:8090",
            "metalake_name": self.metalake_name,
        }
        # to csv
        csv_file = self.fileset_gvfs_location + "/test_pandas/test.csv"
        csv_actual_file = self.fileset_storage_location + "/test_pandas/test.csv"
        data.to_csv(
            csv_file,
            index=False,
            storage_options=storage_options,
        )
        self.assertTrue(fs.exists(csv_file))
        self.assertTrue(self.hdfs.exists(csv_actual_file))

        # read csv
        ds2 = pandas.read_csv(csv_file, storage_options=storage_options)
        self.assertTrue(data.equals(ds2))

    def test_pyarrow(self):
        pyarrow_dir = self.fileset_gvfs_location + "/test_pyarrow"
        pyarrow_actual_dir = self.fileset_storage_location + "/test_pyarrow"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(pyarrow_actual_dir)
        self.assertTrue(self.hdfs.exists(pyarrow_actual_dir))
        self.assertTrue(fs.exists(pyarrow_dir))

        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
        # to parquet
        parquet_file = pyarrow_dir + "/test.parquet"
        data.to_parquet(parquet_file, filesystem=fs)
        self.assertTrue(fs.exists(parquet_file))

        # read as arrow dataset
        arrow_dataset = dt.dataset(parquet_file, filesystem=fs)
        arrow_tb_1 = arrow_dataset.to_table()

        arrow_tb_2 = pa.Table.from_pandas(data)
        self.assertTrue(arrow_tb_1.equals(arrow_tb_2))

        # read as arrow parquet dataset
        arrow_tb_3 = pq.read_table(parquet_file, filesystem=fs)
        self.assertTrue(arrow_tb_3.equals(arrow_tb_2))

    def test_llama_index(self):
        llama_dir = self.fileset_gvfs_location + "/test_llama"
        llama_actual_dir = self.fileset_storage_location + "/test_llama"
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            **self.conf,
        )
        self.hdfs.mkdir(llama_actual_dir)
        self.assertTrue(self.hdfs.exists(llama_actual_dir))
        self.assertTrue(fs.exists(llama_dir))
        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})

        storage_options = {
            "server_uri": "http://localhost:8090",
            "metalake_name": self.metalake_name,
        }
        csv_file = llama_dir + "/test.csv"
        # to csv
        data.to_csv(
            csv_file,
            index=False,
            storage_options=storage_options,
        )
        self.assertTrue(fs.exists(csv_file))
        another_csv_file = llama_dir + "/sub_dir/test1.csv"
        data.to_csv(
            another_csv_file,
            index=False,
            storage_options=storage_options,
        )
        self.assertTrue(fs.exists(another_csv_file))

        reader = SimpleDirectoryReader(
            input_dir=llama_dir[len("gvfs://") :],
            fs=fs,
            recursive=True,  # recursively searches all subdirectories
        )
        documents = reader.load_data()
        self.assertEqual(len(documents), 2)
        doc_1 = documents[0]
        result_1 = [line.strip().split(", ") for line in doc_1.text.split("\n")]
        self.assertEqual(4, len(result_1))
        for row in result_1:
            if row[0] == "A":
                self.assertEqual(row[1], "20")
            elif row[0] == "B":
                self.assertEqual(row[1], "21")
            elif row[0] == "C":
                self.assertEqual(row[1], "19")
            elif row[0] == "D":
                self.assertEqual(row[1], "18")
