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

import os
import random
import string
import unittest
from typing import Optional, Tuple, List, Dict
from unittest import mock
from unittest.mock import patch

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from gravitino import gvfs, NameIdentifier, Fileset
from gravitino.api.credential.credential import Credential
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.storage_handler_provider import StorageHandlerProvider
from gravitino.filesystem.gvfs_storage_handler import StorageHandler
from tests.unittests import mock_base


def generate_unique_random_string(length):
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.sample(characters, length))
    return random_string


class CustomStorageType:
    """Custom storage type for testing"""
    def __init__(self, value: str):
        self.value = value


class CustomStorageHandler(StorageHandler):
    """Custom storage handler implementation for testing"""

    def __init__(self, scheme: str):
        self._scheme = scheme

    def storage_type(self):
        return CustomStorageType(self._scheme)

    def get_filesystem(self, actual_path: Optional[str] = None, **kwargs) -> AbstractFileSystem:
        # Use LocalFileSystem for testing purposes
        return LocalFileSystem(**kwargs)

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        # Return a very long expiration time and the filesystem
        return 9999999999999, self.get_filesystem(actual_path, **kwargs)


class CustomStorageHandlerProvider(StorageHandlerProvider):
    """Custom storage handler provider for testing"""

    def get_storage_handler(self) -> StorageHandler:
        return CustomStorageHandler("custom")

    def scheme(self) -> str:
        return "custom"

    def name(self) -> str:
        return "CustomStorageProvider"


@patch(
    "gravitino.client.generic_fileset.GenericFileset.get_credentials",
    return_value=[],
)
@mock_base.mock_data
class TestCustomStorageHandlerProvider(unittest.TestCase):
    _metalake_name: str = "metalake_demo"
    _server_uri = "http://localhost:9090"
    _local_base_dir_path: str = "file:/tmp/custom_fileset"
    _fileset_dir: str = (
        f"{_local_base_dir_path}/{generate_unique_random_string(10)}/custom_catalog/tmp"
    )

    def setUp(self) -> None:
        local_fs = LocalFileSystem()
        if not local_fs.exists(self._fileset_dir):
            local_fs.mkdir(self._fileset_dir)

    def tearDown(self) -> None:
        local_fs = LocalFileSystem()
        if local_fs.exists(self._local_base_dir_path):
            local_fs.rm(self._local_base_dir_path, recursive=True)

    def test_custom_storage_handler_provider_registration(self, *mock_methods):
        """Test that custom storage handler providers can be registered and used"""
        fileset_storage_location = f"{self._fileset_dir}/test_custom"
        fileset_virtual_location = "fileset/custom_catalog/tmp/test_custom"
        actual_path = fileset_storage_location

        # Mock the provider class path to point to our test class
        provider_class_path = f"{__name__}.CustomStorageHandlerProvider"

        options = {
            GVFSConfig.GVFS_FILESYSTEM_STORAGE_HANDLER_PROVIDERS: provider_class_path
        }

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "test_custom", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)
            self.assertTrue(local_fs.exists(fileset_storage_location))

            # Create GVFS with custom storage handler provider
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                options=options,
                skip_instance_cache=True,
            )

            # Test that the fileset exists and can be accessed
            self.assertTrue(fs.exists(fileset_virtual_location))

    def test_custom_storage_handler_provider_file_operations(self, *mock_methods):
        """Test file operations with custom storage handler provider"""
        fileset_storage_location = f"{self._fileset_dir}/test_custom_ops"
        fileset_virtual_location = "fileset/custom_catalog/tmp/test_custom_ops"
        actual_path = fileset_storage_location

        provider_class_path = f"{__name__}.CustomStorageHandlerProvider"

        options = {
            GVFSConfig.GVFS_FILESYSTEM_STORAGE_HANDLER_PROVIDERS: provider_class_path
        }

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=mock.MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "test_custom_ops", fileset_storage_location
                )
            ),
            get_file_location=mock.MagicMock(return_value=actual_path),
        ):
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)

            # Create test files
            test_file_path = f"{fileset_storage_location}/test_file.txt"
            local_fs.touch(test_file_path)
            self.assertTrue(local_fs.exists(test_file_path))

            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                options=options,
                skip_instance_cache=True,
            )

            # Test listing files
            file_list = fs.ls(fileset_virtual_location, detail=False)
            self.assertEqual(1, len(file_list))
            self.assertTrue(file_list[0].endswith("test_file.txt"))

            # Test file info
            virtual_file_path = f"{fileset_virtual_location}/test_file.txt"
            with patch(
                "gravitino.client.fileset_catalog.FilesetCatalog.get_file_location",
                return_value=test_file_path,
            ):
                file_info = fs.info(virtual_file_path)
                self.assertEqual(file_info["type"], "file")

    def test_invalid_storage_handler_provider(self, *mock_methods):
        """Test error handling for invalid storage handler provider"""
        options = {
            GVFSConfig.GVFS_FILESYSTEM_STORAGE_HANDLER_PROVIDERS: "non.existent.Provider"
        }

        # This should raise an exception during GVFS initialization
        with self.assertRaises(GravitinoRuntimeException):
            gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                options=options,
                skip_instance_cache=True,
            )

    def test_multiple_storage_handler_providers(self, *mock_methods):
        """Test registration of multiple storage handler providers"""
        # Create a second custom provider
        class AnotherCustomStorageHandlerProvider(StorageHandlerProvider):
            def get_storage_handler(self) -> StorageHandler:
                return CustomStorageHandler("another")

            def scheme(self) -> str:
                return "another"

            def name(self) -> str:
                return "AnotherCustomStorageProvider"

        # Add the second provider to the module for dynamic import
        globals()['AnotherCustomStorageHandlerProvider'] = AnotherCustomStorageHandlerProvider

        provider_class_paths = f"{__name__}.CustomStorageHandlerProvider,{__name__}.AnotherCustomStorageHandlerProvider"

        options = {
            GVFSConfig.GVFS_FILESYSTEM_STORAGE_HANDLER_PROVIDERS: provider_class_paths
        }

        # This should successfully register both providers
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri=self._server_uri,
            metalake_name=self._metalake_name,
            options=options,
            skip_instance_cache=True,
        )

        # Verify that both providers are registered by checking the storage handlers
        from gravitino.filesystem.gvfs_storage_handler import get_storage_handler_by_scheme

        # Both schemes should be available
        custom_handler = get_storage_handler_by_scheme("custom")
        self.assertIsNotNone(custom_handler)

        another_handler = get_storage_handler_by_scheme("another")
        self.assertIsNotNone(another_handler)

    def test_storage_handler_provider_interface_validation(self, *mock_methods):
        """Test that non-StorageHandlerProvider classes are rejected"""
        # Create a class that doesn't implement StorageHandlerProvider
        class InvalidProvider:
            pass

        globals()['InvalidProvider'] = InvalidProvider

        options = {
            GVFSConfig.GVFS_FILESYSTEM_STORAGE_HANDLER_PROVIDERS: f"{__name__}.InvalidProvider"
        }

        # This should raise an exception due to interface validation
        with self.assertRaises(GravitinoRuntimeException):
            gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                options=options,
                skip_instance_cache=True,
            )


if __name__ == '__main__':
    unittest.main()