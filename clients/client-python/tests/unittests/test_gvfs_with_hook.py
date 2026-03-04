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
import unittest
from typing import Optional, Dict, List, Union, Any
from unittest import mock
from unittest.mock import patch

from fsspec import Callback
from fsspec.implementations.local import LocalFileSystem

from gravitino.filesystem import gvfs
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.gvfs_hook import GravitinoVirtualFileSystemHook
from tests.unittests import mock_base
from tests.unittests.test_gvfs_with_local import generate_unique_random_string


class MockGVFSHook(GravitinoVirtualFileSystemHook):

    # pylint: disable=too-many-instance-attributes

    def __init__(self):
        self.set_operations_context_called = False
        self.operations = None
        self.ls_called = False
        self.info_called = False
        self.exists_called = False
        self.cp_file_called = False
        self.mv_called = False
        self.rm_called = False
        self.rm_file_called = False
        self.rmdir_called = False
        self.open_called = False
        self.mkdir_called = False
        self.makedirs_called = False
        self.cat_file_called = False
        self.get_file_called = False
        self.created_called = False
        self.modified_called = False
        self.post_ls_called = False
        self.post_info_called = False
        self.post_exists_called = False
        self.post_cp_file_called = False
        self.post_mv_called = False
        self.post_rm_called = False
        self.post_rm_file_called = False
        self.post_rmdir_called = False
        self.post_open_called = False
        self.post_mkdir_called = False
        self.post_makedirs_called = False
        self.post_cat_file_called = False
        self.post_get_file_called = False
        self.post_created_called = False
        self.post_modified_called = False
        # Failure callback tracking
        self.on_ls_failure_called = False
        self.on_info_failure_called = False
        self.on_exists_failure_called = False
        self.on_open_failure_called = False
        self.on_cp_file_failure_called = False
        self.on_mv_failure_called = False
        self.on_rm_failure_called = False
        self.on_rm_file_failure_called = False
        self.on_rmdir_failure_called = False
        self.on_mkdir_failure_called = False
        self.on_makedirs_failure_called = False
        self.on_cat_file_failure_called = False
        self.on_get_file_failure_called = False
        self.on_created_failure_called = False
        self.on_modified_failure_called = False

    def set_operations_context(self, operations):
        self.set_operations_context_called = True
        self.operations = operations

    def initialize(self, config: Optional[Dict[str, str]]):
        pass

    def pre_ls(self, path: str, detail: bool, **kwargs):
        self.ls_called = True
        return path

    def post_ls(
        self, detail: bool, entries: List[Union[str, Dict[str, Any]]], **kwargs
    ) -> List[Union[str, Dict[str, Any]]]:
        self.post_ls_called = True
        return entries

    def pre_info(self, path: str, **kwargs):
        self.info_called = True
        return path

    def post_info(self, info: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        self.post_info_called = True
        return info

    def pre_exists(self, path: str, **kwargs):
        self.exists_called = True
        return path

    def post_exists(self, gvfs_path: str, exists: bool, **kwargs) -> bool:
        self.post_exists_called = True
        return exists

    def pre_cp_file(self, src: str, dst: str, **kwargs):
        self.cp_file_called = True
        return src, dst

    def post_cp_file(self, src_gvfs_path: str, dst_gvfs_path: str, **kwargs):
        self.post_cp_file_called = True

    def pre_mv(self, src: str, dst: str, recursive: bool, maxdepth: int, **kwargs):
        self.mv_called = True
        return src, dst

    def post_mv(
        self,
        src_gvfs_path: str,
        dst_gvfs_path: str,
        recursive: bool,
        maxdepth: int,
        **kwargs,
    ):
        self.post_mv_called = True

    def pre_rm(self, path: str, recursive: bool, maxdepth: int):
        self.rm_called = True
        return path

    def post_rm(self, gvfs_path: str, recursive: bool, maxdepth: int):
        self.post_rm_called = True

    def pre_rm_file(self, path: str):
        self.rm_file_called = True
        return path

    def post_rm_file(self, gvfs_path: str):
        self.post_rm_file_called = True

    def pre_rmdir(self, path: str):
        self.rmdir_called = True
        return path

    def post_rmdir(self, gvfs_path: str):
        self.post_rmdir_called = True

    def pre_open(
        self,
        path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        **kwargs,
    ):
        self.open_called = True
        return path

    def post_open(
        self,
        gvfs_path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        file: Any,
        **kwargs,
    ) -> Any:
        self.post_open_called = True
        return file

    def pre_mkdir(self, path: str, create_parents: bool, **kwargs):
        self.mkdir_called = True
        return path

    def post_mkdir(self, gvfs_path: str, create_parents: bool, **kwargs):
        self.post_mkdir_called = True

    def pre_makedirs(self, path: str, exist_ok: bool):
        self.makedirs_called = True
        return path

    def post_makedirs(self, gvfs_path: str, exist_ok: bool):
        self.post_makedirs_called = True

    def pre_cat_file(self, path: str, start: int, end: int, **kwargs):
        self.cat_file_called = True
        return path

    def post_cat_file(
        self, gvfs_path: str, start: int, end: int, content: Any, **kwargs
    ) -> Any:
        self.post_cat_file_called = True
        return content

    def pre_get_file(
        self, rpath: str, lpath: str, callback: Callback, outfile: str, **kwargs
    ):
        self.get_file_called = True
        return rpath

    def post_get_file(self, gvfs_path: str, local_path: str, outfile: str, **kwargs):
        self.post_get_file_called = True

    def pre_modified(self, path: str) -> str:
        self.modified_called = True
        return path

    def post_modified(self, gvfs_path: str, modified: Any) -> Any:
        self.post_modified_called = True
        return modified

    def pre_created(self, path: str) -> str:
        self.created_called = True
        return path

    def post_created(self, gvfs_path: str, created: Any) -> Any:
        self.post_created_called = True
        return created

    # Failure callback implementations
    def on_ls_failure(self, path: str, exception: Exception, **kwargs):
        self.on_ls_failure_called = True
        super().on_ls_failure(path, exception, **kwargs)

    def on_info_failure(self, path: str, exception: Exception, **kwargs):
        self.on_info_failure_called = True
        super().on_info_failure(path, exception, **kwargs)

    def on_exists_failure(self, path: str, exception: Exception, **kwargs):
        self.on_exists_failure_called = True
        super().on_exists_failure(path, exception, **kwargs)

    def on_open_failure(
        self,
        path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        exception: Exception,
        **kwargs,
    ):
        self.on_open_failure_called = True
        super().on_open_failure(
            path, mode, block_size, cache_options, compression, exception, **kwargs
        )

    def on_cp_file_failure(self, src: str, dst: str, exception: Exception, **kwargs):
        self.on_cp_file_failure_called = True
        super().on_cp_file_failure(src, dst, exception, **kwargs)

    def on_mv_failure(
        self,
        src: str,
        dst: str,
        recursive: bool,
        maxdepth: int,
        exception: Exception,
        **kwargs,
    ):
        self.on_mv_failure_called = True
        super().on_mv_failure(src, dst, recursive, maxdepth, exception, **kwargs)

    def on_rm_failure(
        self, path: str, recursive: bool, maxdepth: int, exception: Exception
    ):
        self.on_rm_failure_called = True
        super().on_rm_failure(path, recursive, maxdepth, exception)

    def on_rm_file_failure(self, path: str, exception: Exception):
        self.on_rm_file_failure_called = True
        super().on_rm_file_failure(path, exception)

    def on_rmdir_failure(self, path: str, exception: Exception):
        self.on_rmdir_failure_called = True
        super().on_rmdir_failure(path, exception)

    def on_mkdir_failure(
        self, path: str, create_parents: bool, exception: Exception, **kwargs
    ):
        self.on_mkdir_failure_called = True
        super().on_mkdir_failure(path, create_parents, exception, **kwargs)

    def on_makedirs_failure(self, path: str, exist_ok: bool, exception: Exception):
        self.on_makedirs_failure_called = True
        super().on_makedirs_failure(path, exist_ok, exception)

    def on_cat_file_failure(
        self, path: str, start: int, end: int, exception: Exception, **kwargs
    ):
        self.on_cat_file_failure_called = True
        super().on_cat_file_failure(path, start, end, exception, **kwargs)

    def on_get_file_failure(
        self,
        rpath: str,
        lpath: str,
        callback: Callback,
        outfile: str,
        exception: Exception,
        **kwargs,
    ):
        self.on_get_file_failure_called = True
        super().on_get_file_failure(
            rpath, lpath, callback, outfile, exception, **kwargs
        )

    def on_created_failure(self, path: str, exception: Exception):
        self.on_created_failure_called = True
        super().on_created_failure(path, exception)

    def on_modified_failure(self, path: str, exception: Exception):
        self.on_modified_failure_called = True
        super().on_modified_failure(path, exception)


@patch(
    "gravitino.client.generic_fileset.GenericFileset.get_credentials",
    return_value=[],
)
@mock_base.mock_data
class TestGVFSHook(unittest.TestCase):
    _local_base_dir_path: str = "file:/tmp/fileset"
    _fileset_dir: str = (
        f"{_local_base_dir_path}/{generate_unique_random_string(10)}/fileset_catalog_with_hook/tmp"
    )

    def setUp(self):
        local_fs = LocalFileSystem()
        if not local_fs.exists(self._fileset_dir):
            local_fs.mkdir(self._fileset_dir)

    def tearDown(self):
        local_fs = LocalFileSystem()
        if local_fs.exists(self._local_base_dir_path):
            local_fs.rm(self._local_base_dir_path, recursive=True)

    def test_hook(self, *mock_method):
        # pylint: disable-msg=too-many-locals
        # pylint: disable=too-many-statements
        fileset_storage_location = f"{self._fileset_dir}/test_location"
        fileset_virtual_path = "fileset/fileset_catalog/tmp/test_location"
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

            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri="http://localhost:9090",
                metalake_name="metalake_demo",
                skip_instance_catch=True,
                options={
                    GVFSConfig.GVFS_FILESYSTEM_HOOK: "tests.unittests.test_gvfs_with_hook.MockGVFSHook"
                },
            )

            # Test that set_operations_context was called during initialization
            self.assertTrue(fs.hook.set_operations_context_called)
            self.assertIsNotNone(fs.hook.operations)
            self.assertEqual(fs.operations, fs.hook.operations)

            # Test pre_exists and post_exists hook
            fs.exists(fileset_virtual_path)
            self.assertTrue(fs.hook.exists_called)
            self.assertTrue(fs.hook.post_exists_called)

            # Test pre_ls and post_ls hook
            fs.ls(fileset_virtual_path)
            self.assertTrue(fs.hook.ls_called)
            self.assertTrue(fs.hook.post_ls_called)

            # Test pre_info and post_info hook
            fs.info(fileset_virtual_path)
            self.assertTrue(fs.hook.info_called)
            self.assertTrue(fs.hook.post_info_called)

        # Test open.
        src_file_name = "src_test_file"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            return_value=f"{fileset_storage_location}/{src_file_name}",
        ):
            test_file_path = f"{fileset_virtual_path}/{src_file_name}"

            # Test pre_open and post_open hook
            with fs.open(test_file_path, "wb") as f:
                f.write(b"test")
            self.assertTrue(fs.hook.open_called)
            self.assertTrue(fs.hook.post_open_called)

            fs.modified(test_file_path)
            self.assertTrue(fs.hook.modified_called)
            self.assertTrue(fs.hook.post_modified_called)

            fs.created(test_file_path)
            self.assertTrue(fs.hook.created_called)
            self.assertTrue(fs.hook.post_created_called)

            # Test pre_cat_file and post_cat_file hook
            fs.cat_file(test_file_path)
            self.assertTrue(fs.hook.cat_file_called)
            self.assertTrue(fs.hook.post_cat_file_called)

            # Test pre_get_file and post_get_file hook
            local_file = (
                f"{self._local_base_dir_path}/{generate_unique_random_string(10)}"
            )
            fs.get_file(test_file_path, local_file)
            self.assertTrue(fs.hook.get_file_called)
            self.assertTrue(fs.hook.post_get_file_called)

        # Test cp.
        dst_file_name = "dst_test_file"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[
                f"{fileset_storage_location}/{src_file_name}",
                f"{fileset_storage_location}/{dst_file_name}",
            ],
        ):
            src_file_path = f"{fileset_virtual_path}/{src_file_name}"
            dst_file_path = f"{fileset_virtual_path}/{dst_file_name}"

            # Test pre_cp_file and post_cp_file hook
            fs.cp_file(src_file_path, dst_file_path)
            self.assertTrue(fs.hook.cp_file_called)
            self.assertTrue(fs.hook.post_cp_file_called)

        # Test mv.
        dst_file_name_1 = "dst_test_file_1"
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[
                f"{fileset_storage_location}/{dst_file_name}",
                f"{fileset_storage_location}/{dst_file_name_1}",
            ],
        ):
            dst_file_path_1 = f"{fileset_virtual_path}/{dst_file_name_1}"

            # Test pre_mv and post_mv hook
            fs.mv(dst_file_path, dst_file_path_1)
            self.assertTrue(fs.hook.mv_called)
            self.assertTrue(fs.hook.post_mv_called)

        # Test rm.
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[
                f"{fileset_storage_location}/{src_file_name}",
                f"{fileset_storage_location}/{dst_file_name_1}",
            ],
        ):
            # Test pre_rm and post_rm hook
            fs.rm(src_file_path)
            self.assertTrue(fs.hook.rm_called)
            self.assertTrue(fs.hook.post_rm_called)

            # Test pre_rm_file and post_rm_file hook
            fs.rm_file(dst_file_path_1)
            self.assertTrue(fs.hook.rm_file_called)
            self.assertTrue(fs.hook.post_rm_file_called)

        # Test mkdir, makedirs, rmdir.
        with patch(
            "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
            side_effect=[
                f"{fileset_storage_location}/test_dir",
                f"{fileset_storage_location}/test_dir_1",
                f"{fileset_storage_location}/test_dir",
            ],
        ):
            # Test pre_mkdir and post_mkdir hook
            fs.mkdir(f"{fileset_virtual_path}/test_dir")
            self.assertTrue(fs.hook.mkdir_called)
            self.assertTrue(fs.hook.post_mkdir_called)

            # Test pre_makedirs and post_makedirs hook
            fs.makedirs(f"{fileset_virtual_path}/test_dir_1")
            self.assertTrue(fs.hook.makedirs_called)
            self.assertTrue(fs.hook.post_makedirs_called)

            # Test pre_rmdir and post_rmdir hook
            fs.rmdir(f"{fileset_virtual_path}/test_dir")
            self.assertTrue(fs.hook.rmdir_called)
            self.assertTrue(fs.hook.post_rmdir_called)

    def test_failure_callbacks(self, *mock_method):
        """Test that failure callbacks are invoked when operations fail."""
        fileset_storage_location = f"{self._fileset_dir}/test_failure_location"
        fileset_virtual_path = "fileset/fileset_catalog/tmp/test_failure_location"

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "fileset", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=fileset_storage_location),
        ):
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri="http://localhost:9090",
                metalake_name="metalake_demo",
                skip_instance_catch=True,
                options={
                    GVFSConfig.GVFS_FILESYSTEM_HOOK: "tests.unittests.test_gvfs_with_hook.MockGVFSHook"
                },
            )

            # Test on_ls_failure
            with patch.object(
                fs.operations, "ls", side_effect=RuntimeError("ls failed")
            ):
                with self.assertRaises(RuntimeError):
                    fs.ls(fileset_virtual_path)
                self.assertTrue(
                    fs.hook.on_ls_failure_called,
                    "on_ls_failure should be called when ls operation fails",
                )

            # Test on_info_failure
            with patch.object(
                fs.operations, "info", side_effect=RuntimeError("info failed")
            ):
                with self.assertRaises(RuntimeError):
                    fs.info(fileset_virtual_path)
                self.assertTrue(
                    fs.hook.on_info_failure_called,
                    "on_info_failure should be called when info operation fails",
                )

            # Test on_open_failure
            with patch.object(
                fs.operations, "open", side_effect=RuntimeError("open failed")
            ):
                with self.assertRaises(RuntimeError):
                    fs.open(fileset_virtual_path, mode="rb")
                self.assertTrue(
                    fs.hook.on_open_failure_called,
                    "on_open_failure should be called when open operation fails",
                )

            # Test on_mkdir_failure
            with patch.object(
                fs.operations, "mkdir", side_effect=RuntimeError("mkdir failed")
            ):
                with self.assertRaises(RuntimeError):
                    fs.mkdir(fileset_virtual_path)
                self.assertTrue(
                    fs.hook.on_mkdir_failure_called,
                    "on_mkdir_failure should be called when mkdir operation fails",
                )

            # Test on_rm_file_failure
            with patch.object(
                fs.operations, "rm_file", side_effect=RuntimeError("rm_file failed")
            ):
                with self.assertRaises(RuntimeError):
                    fs.rm_file(fileset_virtual_path)
                self.assertTrue(
                    fs.hook.on_rm_file_failure_called,
                    "on_rm_file_failure should be called when rm_file operation fails",
                )
