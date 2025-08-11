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
import base64
import os
import random
import string
import time
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import patch

import pandas
import pyarrow as pa
import pyarrow.dataset as dt
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem

from gravitino import gvfs, NameIdentifier, Fileset
from gravitino.auth.auth_constants import AuthConstants
from gravitino.constants.timeout import TIMEOUT
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    IllegalArgumentException,
    BadRequestException,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.gvfs_storage_handler import LOCA_HANDLER
from gravitino.filesystem.gvfs_utils import extract_identifier, to_gvfs_path_prefix
from tests.unittests import mock_base
from tests.unittests.auth.mock_base import (
    mock_jwt,
    GENERATED_TIME,
    mock_authentication_with_error_authentication_type,
    mock_authentication_invalid_grant_error,
)


# pylint: disable=protected-access,too-many-lines,too-many-locals


# pylint: disable=C0302
def generate_unique_random_string(length):
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.sample(characters, length))
    return random_string


@patch(
    "gravitino.client.generic_fileset.GenericFileset.get_credentials",
    return_value=[],
)
@mock_base.mock_data
class TestLocalFilesystem(unittest.TestCase):
    _metalake_name: str = "metalake_demo"
    _server_uri = "http://localhost:9090"
    _local_base_dir_path: str = "file:/tmp/fileset"
    _fileset_dir: str = (
        f"{_local_base_dir_path}/{generate_unique_random_string(10)}/fileset_catalog/tmp"
    )

    def setUp(self) -> None:
        local_fs = LocalFileSystem()
        if not local_fs.exists(self._fileset_dir):
            local_fs.mkdir(self._fileset_dir)

    def tearDown(self) -> None:
        local_fs = LocalFileSystem()
        if local_fs.exists(self._local_base_dir_path):
            local_fs.rm(self._local_base_dir_path, recursive=True)

    def test_request_headers(self, *mock_methods):
        options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_CLIENT_REQUEST_HEADER_PREFIX}k1": "v1",
        }
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            options=options,
            skip_instance_cache=True,
        )
        headers = fs._operations._client._rest_client.request_headers
        self.assertEqual(headers["k1"], "v1")

    def test_request_timeout(self, *mock_methods):
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            skip_instance_cache=True,
        )
        self.assertEqual(fs._operations._client._rest_client.timeout, TIMEOUT)

        options = {
            f"{GVFSConfig.GVFS_FILESYSTEM_CLIENT_CONFIG_PREFIX}request_timeout": 60,
        }
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            options=options,
            skip_instance_cache=True,
        )
        self.assertEqual(fs._operations._client._rest_client.timeout, 60)

    def test_cache(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cache"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cache"
        actual_path = fileset_storage_location
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "test_cache", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)
            self.assertTrue(local_fs.exists(fileset_storage_location))
            options = {GVFSConfig.CACHE_SIZE: 1, GVFSConfig.CACHE_EXPIRED_TIME: 1}
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                options=options,
                skip_instance_cache=True,
            )
            self.assertTrue(fs.exists(fileset_virtual_location))
            # wait 2 seconds
            time.sleep(2)
            name_identifier = NameIdentifier.of(
                self._metalake_name, "fileset_catalog", "tmp", "test_cache"
            )
            self.assertIsNone(
                fs._operations._filesystem_cache.get(
                    (name_identifier, Fileset.LOCATION_NAME_UNKNOWN)
                )
            )

    def test_simple_auth(self, *mock_methods):
        options = {"auth_type": "simple"}
        current_user = (
            None if os.environ.get("user.name") is None else os.environ["user.name"]
        )
        user = "test_gvfs"
        os.environ["user.name"] = user
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            options=options,
            skip_instance_cache=True,
        )
        token = fs._operations._client._rest_client.auth_data_provider.get_token_data()
        token_string = base64.b64decode(
            token.decode("utf-8")[len(AuthConstants.AUTHORIZATION_BASIC_HEADER) :]
        ).decode("utf-8")
        self.assertEqual(f"{user}:dummy", token_string)
        if current_user is not None:
            os.environ["user.name"] = current_user

    def test_oauth2_auth(self, *mock_methods):
        fs_options = {
            GVFSConfig.AUTH_TYPE: GVFSConfig.OAUTH2_AUTH_TYPE,
            GVFSConfig.OAUTH2_SERVER_URI: "http://127.0.0.1:1082",
            GVFSConfig.OAUTH2_CREDENTIAL: "xx:xx",
            GVFSConfig.OAUTH2_SCOPE: "test",
            GVFSConfig.OAUTH2_PATH: "token/test",
        }
        # test auth normally
        mocked_jwt = mock_jwt(
            sub="gravitino", exp=GENERATED_TIME + 10000, aud="service1"
        )
        with patch(
            "gravitino.auth.default_oauth2_token_provider.DefaultOAuth2TokenProvider._get_access_token",
            return_value=mocked_jwt,
        ), patch(
            "gravitino.auth.default_oauth2_token_provider.DefaultOAuth2TokenProvider._fetch_token",
            return_value=mocked_jwt,
        ):
            fileset_storage_location = f"{self._fileset_dir}/test_oauth2_auth"
            fileset_virtual_location = "fileset/fileset_catalog/tmp/test_oauth2_auth"
            actual_path = fileset_storage_location
            with patch.multiple(
                "gravitino.client.fileset_catalog.FilesetCatalog",
                load_fileset=mock.MagicMock(
                    return_value=mock_base.mock_load_fileset(
                        "fileset", fileset_storage_location
                    )
                ),
                get_file_location=mock.MagicMock(return_value=actual_path),
            ):
                local_fs = LocalFileSystem()
                local_fs.mkdir(fileset_storage_location)
                sub_dir_path = f"{fileset_storage_location}/test_1"
                local_fs.mkdir(sub_dir_path)
                self.assertTrue(local_fs.exists(sub_dir_path))
                sub_file_path = f"{fileset_storage_location}/test_file_1.par"
                local_fs.touch(sub_file_path)
                self.assertTrue(local_fs.exists(sub_file_path))
                fs = gvfs.GravitinoVirtualFileSystem(
                    server_uri=self._server_uri,
                    metalake_name=self._metalake_name,
                    options=fs_options,
                    skip_instance_cache=True,
                )
                # should not raise exception
                self.assertTrue(fs.exists(fileset_virtual_location))

        # test error authentication type
        with patch(
            "gravitino.utils.http_client.HTTPClient.post_form",
            return_value=mock_authentication_with_error_authentication_type(),
        ):
            with self.assertRaises(IllegalArgumentException):
                gvfs.GravitinoVirtualFileSystem(
                    server_uri=self._server_uri,
                    metalake_name=self._metalake_name,
                    options=fs_options,
                    skip_instance_cache=True,
                )

        # test bad request
        with patch(
            "gravitino.utils.http_client.HTTPClient._make_request",
            return_value=mock_authentication_invalid_grant_error(),
        ):
            with self.assertRaises(BadRequestException):
                gvfs.GravitinoVirtualFileSystem(
                    server_uri=self._server_uri,
                    metalake_name=self._metalake_name,
                    options=fs_options,
                    skip_instance_cache=True,
                )

    def test_ls(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_ls"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_ls"
        actual_path = fileset_storage_location
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)
            sub_dir_path = f"{fileset_storage_location}/test_1"
            local_fs.mkdir(sub_dir_path)
            self.assertTrue(local_fs.exists(sub_dir_path))
            sub_file_path = f"{fileset_storage_location}/test_file_1.par"
            local_fs.touch(sub_file_path)
            self.assertTrue(local_fs.exists(sub_file_path))

            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                skip_instance_cache=True,
            )
            self.assertTrue(fs.exists(fileset_virtual_location))

            # test detail = false
            file_list_without_detail = fs.ls(fileset_virtual_location, detail=False)
            file_list_without_detail.sort()
            self.assertEqual(2, len(file_list_without_detail))
            self.assertEqual(
                file_list_without_detail[0], f"{fileset_virtual_location}/test_1"
            )
            self.assertEqual(
                file_list_without_detail[1],
                f"{fileset_virtual_location}/test_file_1.par",
            )

            # test detail = true
            file_list_with_detail = fs.ls(fileset_virtual_location, detail=True)
            file_list_with_detail.sort(key=lambda x: x["name"])
            self.assertEqual(2, len(file_list_with_detail))
            self.assertEqual(
                file_list_with_detail[0]["name"], f"{fileset_virtual_location}/test_1"
            )
            self.assertEqual(
                file_list_with_detail[1]["name"],
                f"{fileset_virtual_location}/test_file_1.par",
            )

    def test_info(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_info"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_info"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/test_1"
        local_fs.mkdir(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        dir_virtual_path = fileset_virtual_location + "/test_1"
        actual_path = fileset_storage_location + "/test_1"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path,
        ):
            dir_info = fs.info(dir_virtual_path)
            self.assertEqual(dir_info["name"], dir_virtual_path)

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path,
        ):
            file_info = fs.info(file_virtual_path)
            self.assertEqual(file_info["name"], file_virtual_path)

    def test_exist(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_exist"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_exist"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/test_1"
        local_fs.mkdir(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        dir_virtual_path = fileset_virtual_location + "/test_1"
        actual_path = fileset_storage_location + "/test_1"
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(dir_virtual_path))

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path = fileset_storage_location + "/test_file_1.par"
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(file_virtual_path))

    def test_cp_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cp_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cp_file"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        with local_fs.open(sub_file_path, "wb") as f:
            f.write(b"test_file_1")

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        src_actual_path = fileset_storage_location + "/test_file_1.par"
        dst_actual_path = fileset_storage_location + "/test_cp_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[
                src_actual_path,
                src_actual_path,
                dst_actual_path,
                dst_actual_path,
            ],
        ), patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.load_fileset",
            return_value=mock_base.mock_load_fileset(
                "fileset", fileset_storage_location
            ),
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            cp_file_virtual_path = fileset_virtual_location + "/test_cp_file_1.par"
            fs.cp_file(file_virtual_path, cp_file_virtual_path)
            self.assertTrue(fs.exists(cp_file_virtual_path))
            with local_fs.open(sub_file_path, "rb") as f:
                result = f.read()
            self.assertEqual(b"test_file_1", result)

        # test invalid dst path
        cp_file_invalid_virtual_path = (
            "fileset/fileset_catalog/tmp/invalid_fileset/test_cp_file_1.par"
        )
        with self.assertRaises(GravitinoRuntimeException):
            fs.cp_file(file_virtual_path, cp_file_invalid_virtual_path)

    def test_mv(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_mv"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_mv"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        another_dir_path = f"{fileset_storage_location}/another_dir"
        local_fs.mkdirs(another_dir_path)
        self.assertTrue(local_fs.exists(another_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        src_actual_path = fileset_storage_location + "/test_file_1.par"
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=src_actual_path),
        ):
            self.assertTrue(fs.exists(file_virtual_path))

        mv_file_virtual_path = fileset_virtual_location + "/test_cp_file_1.par"
        dst_actual_path = fileset_storage_location + "/test_cp_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[src_actual_path, dst_actual_path, dst_actual_path],
        ), patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.load_fileset",
            return_value=mock_base.mock_load_fileset(
                "fileset", fileset_storage_location
            ),
        ):
            fs.mv(file_virtual_path, mv_file_virtual_path)
            self.assertTrue(fs.exists(mv_file_virtual_path))

        mv_another_dir_virtual_path = (
            fileset_virtual_location + "/another_dir/test_file_2.par"
        )
        dst_actual_path1 = fileset_storage_location + "/another_dir/test_file_2.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[dst_actual_path, dst_actual_path1, dst_actual_path1],
        ), patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.load_fileset",
            return_value=mock_base.mock_load_fileset(
                "fileset", fileset_storage_location
            ),
        ):
            fs.mv(mv_file_virtual_path, mv_another_dir_virtual_path)
            self.assertTrue(fs.exists(mv_another_dir_virtual_path))

        # test not exist dir
        not_exist_dst_dir_path = fileset_virtual_location + "/not_exist/test_file_2.par"
        dst_actual_path2 = fileset_storage_location + "/not_exist/test_file_2.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[dst_actual_path1, dst_actual_path2],
        ), patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.load_fileset",
            return_value=mock_base.mock_load_fileset(
                "fileset", fileset_storage_location
            ),
        ):
            with self.assertRaises(FileNotFoundError):
                fs.mv(path1=mv_another_dir_virtual_path, path2=not_exist_dst_dir_path)

        # test invalid dst path
        mv_file_invalid_virtual_path = (
            "fileset/fileset_catalog/tmp/invalid_fileset/test_cp_file_1.par"
        )
        with self.assertRaises(GravitinoRuntimeException):
            fs.mv(path1=file_virtual_path, path2=mv_file_invalid_virtual_path)

    def test_rm(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_rm"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_rm"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test delete file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            fs.rm(file_virtual_path)
            self.assertFalse(fs.exists(file_virtual_path))

        # test delete dir with recursive = false
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises(ValueError):
                fs.rm(dir_virtual_path, recursive=False)

            # test delete dir with recursive = true
            fs.rm(dir_virtual_path, recursive=True)
            self.assertFalse(fs.exists(dir_virtual_path))

    def test_rm_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_rm_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_rm_file"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test delete file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            fs.rm_file(file_virtual_path)
            self.assertFalse(fs.exists(file_virtual_path))

        # test delete dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises((IsADirectoryError, PermissionError)):
                fs.rm_file(dir_virtual_path)

    def test_rmdir(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_rmdir"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_rmdir"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test delete file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with self.assertRaises(NotADirectoryError):
                fs.rmdir(file_virtual_path)

        # test delete dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            fs.rmdir(dir_virtual_path)
            self.assertFalse(fs.exists(dir_virtual_path))

    def test_open(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_open"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_open"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test open and write file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with fs.open(file_virtual_path, mode="wb") as f:
                f.write(b"test_open_write")
            self.assertTrue(fs.info(file_virtual_path)["size"] > 0)

            # test open and read file
            with fs.open(file_virtual_path, mode="rb") as f:
                self.assertEqual(b"test_open_write", f.read())

        # test open dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises(IsADirectoryError):
                fs.open(dir_virtual_path)

    def test_mkdir(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_mkdir"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_mkdir"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

            # test mkdir dir which exists
            existed_dir_virtual_path = fileset_virtual_location
            self.assertTrue(fs.exists(existed_dir_virtual_path))
            with self.assertRaises(FileExistsError):
                fs.mkdir(existed_dir_virtual_path)

        # test mkdir dir with create_parents = false
        parent_not_exist_virtual_path = fileset_virtual_location + "/not_exist/sub_dir"
        actual_path1 = fileset_storage_location + "/not_exist/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertFalse(fs.exists(parent_not_exist_virtual_path))
            with self.assertRaises(FileNotFoundError):
                fs.mkdir(parent_not_exist_virtual_path, create_parents=False)

        # test mkdir dir with create_parents = true
        parent_not_exist_virtual_path2 = fileset_virtual_location + "/not_exist/sub_dir"
        actual_path2 = fileset_storage_location + "/not_exist/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            self.assertFalse(fs.exists(parent_not_exist_virtual_path2))
            fs.mkdir(parent_not_exist_virtual_path2, create_parents=True)
            self.assertTrue(fs.exists(parent_not_exist_virtual_path2))

    def test_makedirs(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_makedirs"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_makedirs"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

            # test mkdir dir which exists
            existed_dir_virtual_path = fileset_virtual_location
            self.assertTrue(fs.exists(existed_dir_virtual_path))
            with self.assertRaises(FileExistsError):
                fs.mkdirs(existed_dir_virtual_path)

        # test mkdir dir not exist
        parent_not_exist_virtual_path = fileset_virtual_location + "/not_exist/sub_dir"
        actual_path1 = fileset_storage_location + "/not_exist/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertFalse(fs.exists(parent_not_exist_virtual_path))
            fs.makedirs(parent_not_exist_virtual_path)
            self.assertTrue(fs.exists(parent_not_exist_virtual_path))

    def test_created(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_created"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_created"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test mkdir dir which exists
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path1 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            self.assertIsNotNone(fs.created(dir_virtual_path))

    def test_modified(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_modified"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_modified"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test mkdir dir which exists
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path1 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            self.assertIsNotNone(fs.modified(dir_virtual_path))

    def test_cat_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cat_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cat_file"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test open and write file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with fs.open(file_virtual_path, mode="wb") as f:
                f.write(b"test_cat_file")
            self.assertTrue(fs.info(file_virtual_path)["size"] > 0)

            # test cat file
            content = fs.cat_file(file_virtual_path)
            self.assertEqual(b"test_cat_file", content)

        # test cat dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises(IsADirectoryError):
                fs.cat_file(dir_virtual_path)

    def test_get_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_get_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_get_file"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test open and write file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with fs.open(file_virtual_path, mode="wb") as f:
                f.write(b"test_get_file")
            self.assertTrue(fs.info(file_virtual_path)["size"] > 0)

            # test get file
            local_path = self._fileset_dir + "/local_file.par"
            local_fs.touch(local_path)
            self.assertTrue(local_fs.exists(local_path))
            fs.get_file(file_virtual_path, local_path)
            self.assertEqual(b"test_get_file", local_fs.cat_file(local_path))

        # test get a dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            local_path = self._fileset_dir + "/local_dir"
            self.assertTrue(fs.exists(dir_virtual_path))
            fs.get_file(dir_virtual_path, local_path)
            self.assertTrue(local_fs.exists(local_path))

        # test get a file to a remote file
        remote_path = "gvfs://" + fileset_virtual_location + "/test_file_2.par"
        with self.assertRaises(GravitinoRuntimeException):
            fs.get_file(file_virtual_path, remote_path)

    def test_convert_actual_path(self, *mock_methods):
        storage_location = "file:/fileset/test_f1"
        virtual_location = to_gvfs_path_prefix(
            NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "test_f1")
        )
        # test actual path not start with storage location
        actual_path = "/not_start_with_storage/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            LOCA_HANDLER.actual_path_to_gvfs_path(
                actual_path, storage_location, virtual_location
            )

        # test actual path start with storage location
        actual_path = "/fileset/test_f1/actual_path"
        virtual_path = LOCA_HANDLER.actual_path_to_gvfs_path(
            actual_path, storage_location, virtual_location
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", virtual_path
        )

        # test convert actual local path
        storage_location = "file:/tmp/fileset/test_f1"
        virtual_location = to_gvfs_path_prefix(
            NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "test_f1")
        )
        # test actual path not start with storage location
        actual_path = "/not_start_with_storage/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            LOCA_HANDLER.actual_path_to_gvfs_path(
                actual_path, storage_location, virtual_location
            )

        # test actual path start with storage location
        actual_path = "/tmp/fileset/test_f1/actual_path"
        virtual_path = LOCA_HANDLER.actual_path_to_gvfs_path(
            actual_path, storage_location, virtual_location
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", virtual_path
        )

        # test storage location without "/"
        actual_path = "/tmp/test_convert_actual_path/sub_dir/1.parquet"
        storage_location = "file:/tmp/test_convert_actual_path"
        virtual_location = to_gvfs_path_prefix(
            NameIdentifier.of(
                "test_metalake", "catalog", "schema", "test_convert_actual_path"
            )
        )

        virtual_path = LOCA_HANDLER.actual_path_to_gvfs_path(
            actual_path, storage_location, virtual_location
        )
        self.assertEqual(
            "fileset/catalog/schema/test_convert_actual_path/sub_dir/1.parquet",
            virtual_path,
        )

        # test storage location with "/"
        actual_path = "/tmp/test_convert_actual_path/sub_dir/1.parquet"
        storage_location = "file:/tmp/test_convert_actual_path/"
        virtual_location = to_gvfs_path_prefix(
            NameIdentifier.of(
                "test_metalake", "catalog", "schema", "test_convert_actual_path"
            )
        )

        virtual_path = LOCA_HANDLER.actual_path_to_gvfs_path(
            actual_path, storage_location, virtual_location
        )
        self.assertEqual(
            "fileset/catalog/schema/test_convert_actual_path/sub_dir/1.parquet",
            virtual_path,
        )

    def test_convert_info(self, *mock_methods):
        # test actual path not start with storage location
        entry = {
            "name": "/not_start_with_storage/ttt",
            "size": 1,
            "type": "file",
            "mtime": datetime.now(),
        }
        storage_location = "file:/fileset/test_f1"
        gvfs_path_prefix = to_gvfs_path_prefix(
            NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "test_f1")
        )
        with self.assertRaises(GravitinoRuntimeException):
            LOCA_HANDLER.actual_info_to_gvfs_info(
                entry, storage_location, gvfs_path_prefix
            )

        # test actual path start with storage location
        entry = {
            "name": "/fileset/test_f1/actual_path",
            "size": 1,
            "type": "file",
            "mtime": datetime.now(),
        }
        info = LOCA_HANDLER.actual_info_to_gvfs_info(
            entry, storage_location, gvfs_path_prefix
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", info["name"]
        )

        # test convert actual local path
        # test actual path not start with storage location
        entry = {
            "name": "/not_start_with_storage/ttt",
            "size": 1,
            "type": "file",
            "mtime": datetime.now(),
        }
        storage_location = "file:/tmp/fileset/test_f1"
        gvfs_path_prefix = to_gvfs_path_prefix(
            NameIdentifier.of("test_metalake", "test_catalog", "test_schema", "test_f1")
        )
        with self.assertRaises(GravitinoRuntimeException):
            LOCA_HANDLER.actual_info_to_gvfs_info(
                entry, storage_location, gvfs_path_prefix
            )

        # test actual path start with storage location
        entry = {
            "name": "/tmp/fileset/test_f1/actual_path",
            "size": 1,
            "type": "file",
            "mtime": datetime.now(),
        }
        info = LOCA_HANDLER.actual_info_to_gvfs_info(
            entry, storage_location, gvfs_path_prefix
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", info["name"]
        )

    def test_extract_identifier(self, *mock_methods):
        with self.assertRaises(GravitinoRuntimeException):
            extract_identifier(self._metalake_name, path=None)

        invalid_path = "s3://bucket_1/test_catalog/schema/fileset/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            extract_identifier(self._metalake_name, path=invalid_path)

        valid_path = "fileset/test_catalog/schema/fileset/ttt"
        identifier: NameIdentifier = extract_identifier(
            self._metalake_name, path=valid_path
        )
        self.assertEqual(self._metalake_name, identifier.namespace().level(0))
        self.assertEqual("test_catalog", identifier.namespace().level(1))
        self.assertEqual("schema", identifier.namespace().level(2))
        self.assertEqual("fileset", identifier.name())

    def test_pandas(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_pandas"
        fileset_virtual_location = "gvfs://fileset/fileset_catalog/tmp/test_pandas"
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name="test_metalake",
            skip_instance_cache=True,
        )
        actual_path = fileset_storage_location + "/test.parquet"
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            # to parquet
            data.to_parquet(fileset_virtual_location + "/test.parquet", filesystem=fs)
            self.assertTrue(local_fs.exists(fileset_storage_location + "/test.parquet"))

            # read parquet
            ds1 = pandas.read_parquet(
                path=fileset_virtual_location + "/test.parquet", filesystem=fs
            )
            self.assertTrue(data.equals(ds1))
        storage_options = {
            "server_uri": "http://localhost:8090",
            "metalake_name": "test_metalake",
        }

        actual_path1 = fileset_storage_location
        actual_path2 = fileset_storage_location + "/test.csv"

        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[actual_path1, actual_path2, actual_path2],
        ), patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.load_fileset",
            return_value=mock_base.mock_load_fileset(
                "fileset", fileset_storage_location
            ),
        ):
            # to csv
            data.to_csv(
                fileset_virtual_location + "/test.csv",
                index=False,
                storage_options=storage_options,
            )
            self.assertTrue(local_fs.exists(fileset_storage_location + "/test.csv"))

            # read csv
            ds2 = pandas.read_csv(
                fileset_virtual_location + "/test.csv", storage_options=storage_options
            )
            self.assertTrue(data.equals(ds2))

    def test_pyarrow(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_pyarrow"
        fileset_virtual_location = "gvfs://fileset/fileset_catalog/tmp/test_pyarrow"
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090",
            metalake_name="test_metalake",
            skip_instance_cache=True,
        )
        actual_path = fileset_storage_location + "/test.parquet"
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            # to parquet
            data.to_parquet(fileset_virtual_location + "/test.parquet", filesystem=fs)
            self.assertTrue(local_fs.exists(fileset_storage_location + "/test.parquet"))

            # read as arrow dataset
            arrow_dataset = dt.dataset(
                fileset_virtual_location + "/test.parquet", filesystem=fs
            )
            arrow_tb_1 = arrow_dataset.to_table()
            arrow_tb_2 = pa.Table.from_pandas(data)
            self.assertTrue(arrow_tb_1.equals(arrow_tb_2))

            # read as arrow parquet dataset
            arrow_tb_3 = pq.read_table(
                fileset_virtual_location + "/test.parquet", filesystem=fs
            )
            self.assertTrue(arrow_tb_3.equals(arrow_tb_2))

    def test_location_with_tailing_slash(self, *mock_methods):
        # storage location is ending with a "/"
        fileset_storage_location = (
            f"{self._fileset_dir}/test_location_with_tailing_slash/"
        )
        fileset_virtual_location = (
            "fileset/fileset_catalog/tmp/test_location_with_tailing_slash"
        )
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}test_1"
        local_fs.mkdir(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))
        sub_file_path = f"{sub_dir_path}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        actual_path = fileset_storage_location
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            skip_instance_cache=True,
        )
        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        dir_virtual_path = fileset_virtual_location + "/test_1"
        actual_path1 = fileset_storage_location + "test_1"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path1,
        ):
            dir_info = fs.info(dir_virtual_path)
            self.assertEqual(dir_info["name"], dir_virtual_path)

        file_virtual_path = fileset_virtual_location + "/test_1/test_file_1.par"
        actual_path2 = fileset_storage_location + "test_1/test_file_1.par"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path2,
        ):
            file_info = fs.info(file_virtual_path)
            self.assertEqual(file_info["name"], file_virtual_path)

        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=actual_path,
        ):
            file_status = fs.ls(fileset_virtual_location, detail=True)
            for status in file_status:
                if status["name"].endswith("test_1"):
                    self.assertEqual(status["name"], dir_virtual_path)
                elif status["name"].endswith("test_file_1.par"):
                    self.assertEqual(status["name"], file_virtual_path)
                else:
                    raise GravitinoRuntimeException("Unexpected file found")
