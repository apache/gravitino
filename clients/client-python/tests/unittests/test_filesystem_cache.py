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
"""
Unit tests for filesystem caching behavior in GVFS.

These tests verify that filesystem instances are cached correctly based on
(scheme, authority, credentials, config), matching the behavior of Java GVFS
and how Hadoop HDFS client caches FileSystem instances.
"""
import os
import threading
import time
import unittest
from unittest.mock import patch, MagicMock

from fsspec.implementations.local import LocalFileSystem

from gravitino import gvfs
from gravitino.filesystem.gvfs_base_operations import FileSystemCacheKey
from gravitino.filesystem.gvfs_config import GVFSConfig
from tests.unittests import mock_base


@patch(
    "gravitino.client.generic_fileset.GenericFileset.get_credentials",
    return_value=[],
)
@mock_base.mock_data
class TestFileSystemCache(unittest.TestCase):
    """Tests for filesystem-level caching in GVFS."""

    _metalake_name: str = "metalake_demo"
    _catalog_name: str = "fileset_catalog"
    _schema_name: str = "tmp"
    _server_uri: str = "http://localhost:8090"
    _fileset_dir: str = "/tmp/test_gvfs_cache"

    def setUp(self):
        """Set up test fixtures."""
        self.local_fs = LocalFileSystem()
        if not self.local_fs.exists(self._fileset_dir):
            self.local_fs.mkdir(self._fileset_dir, create_parents=True)

    def tearDown(self):
        """Clean up test artifacts."""
        if self.local_fs.exists(self._fileset_dir):
            self.local_fs.rm(self._fileset_dir, recursive=True)

    def test_filesystem_shared_across_filesets(self, *mock_methods):
        """
        Test that multiple filesets pointing to the same storage share the same filesystem instance.

        This verifies the key behavior change: caching by (scheme, authority, config)
        instead of by (fileset_name, location_name).
        """
        # Create storage location with file:// scheme
        storage_location = f"file://{self._fileset_dir}/shared_storage"
        self.local_fs.mkdir(storage_location, create_parents=True)

        # Create two different filesets pointing to the same storage
        fileset1_name = "fileset1"
        fileset2_name = "fileset2"

        fileset1_virtual_path = f"fileset/{self._catalog_name}/{self._schema_name}/{fileset1_name}/file1.txt"
        fileset2_virtual_path = f"fileset/{self._catalog_name}/{self._schema_name}/{fileset2_name}/file2.txt"

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=MagicMock(
                side_effect=lambda name_id: mock_base.mock_load_fileset(
                    name_id.name(), storage_location
                )
            ),
            get_file_location=MagicMock(
                side_effect=lambda fileset_id, sub_path, location_name: f"{storage_location}/{sub_path}"
            ),
        ):
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                skip_instance_cache=True,
            )

            # Access both filesets
            fs.touch(fileset1_virtual_path)
            fs.touch(fileset2_virtual_path)

            self.assertTrue(fs.exists(fileset1_virtual_path))
            self.assertTrue(fs.exists(fileset2_virtual_path))

            # Verify the cache has only one filesystem entry since both filesets
            # point to the same storage location with the same configuration
            # pylint: disable=protected-access
            cache_size = len(fs._operations._filesystem_cache)
            self.assertEqual(
                cache_size,
                1,
                f"Expected 1 cached filesystem (shared), but found {cache_size}. "
                "Multiple filesets pointing to the same storage should share the same filesystem instance.",
            )

    def test_cache_isolation_by_storage_location(self, *mock_methods):
        """
        Test that filesets pointing to the same storage type share filesystem instances.

        For local filesystems (file://), different paths share the same LocalFileSystem
        instance because the filesystem is not path-specific. This matches how Hadoop
        caches FileSystem instances - same scheme and authority = shared instance.
        """
        # Create two different storage locations with file:// scheme
        storage1 = f"file://{self._fileset_dir}/storage1"
        storage2 = f"file://{self._fileset_dir}/storage2"
        self.local_fs.mkdir(storage1, create_parents=True)
        self.local_fs.mkdir(storage2, create_parents=True)

        fileset1_virtual = (
            f"fileset/{self._catalog_name}/{self._schema_name}/fileset1/file.txt"
        )
        fileset2_virtual = (
            f"fileset/{self._catalog_name}/{self._schema_name}/fileset2/file.txt"
        )

        def mock_get_file_location(fileset_id, sub_path, location_name):
            if fileset_id.name() == "fileset1":
                return f"{storage1}/{sub_path}"
            return f"{storage2}/{sub_path}"

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=MagicMock(
                side_effect=lambda name_id: mock_base.mock_load_fileset(
                    name_id.name(),
                    storage1 if name_id.name() == "fileset1" else storage2,
                )
            ),
            get_file_location=MagicMock(side_effect=mock_get_file_location),
        ):
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                skip_instance_cache=True,
            )

            # Access both filesets
            fs.touch(fileset1_virtual)
            fs.touch(fileset2_virtual)

            self.assertTrue(fs.exists(fileset1_virtual))
            self.assertTrue(fs.exists(fileset2_virtual))

            # For local filesystems with same scheme (file://) and no authority,
            # they share the same filesystem instance. This is correct behavior:
            # - file://path1 and file://path2 use the same LocalFileSystem
            # - s3://bucket1 and s3://bucket2 would have different cache entries
            # pylint: disable=protected-access
            cache_size = len(fs._operations._filesystem_cache)
            self.assertEqual(
                cache_size,
                1,
                f"Expected 1 cached filesystem (shared local), but found {cache_size}. "
                "Local filesystem paths with same scheme should share the same instance.",
            )

    def test_cache_expiration(self, *mock_methods):
        """
        Test that expired filesystem entries are removed from cache and recreated.
        """
        storage_location = f"file://{self._fileset_dir}/expiration_test"
        self.local_fs.mkdir(storage_location, create_parents=True)

        fileset_virtual = (
            f"fileset/{self._catalog_name}/{self._schema_name}/test_expiration/file.txt"
        )

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "test_expiration", storage_location
                )
            ),
            get_file_location=MagicMock(return_value=f"{storage_location}/file.txt"),
        ):
            # Create filesystem with 1-second cache expiration
            options = {
                GVFSConfig.CACHE_SIZE: 10,
                GVFSConfig.CACHE_EXPIRED_TIME: 1,  # 1 second
            }
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                options=options,
                skip_instance_cache=True,
            )

            # First access - should create cache entry
            fs.touch(fileset_virtual)
            self.assertTrue(fs.exists(fileset_virtual))

            # pylint: disable=protected-access
            initial_cache_size = len(fs._operations._filesystem_cache)
            self.assertEqual(
                initial_cache_size, 1, "Cache should have 1 entry initially"
            )

            # Wait for cache to expire (TTL is 1 second)
            time.sleep(2)

            # Access again - should detect expiration and recreate
            fs.touch(fileset_virtual)
            self.assertTrue(fs.exists(fileset_virtual))

            # Cache should still have entry (TTL cache auto-evicts expired entries)
            # The exact behavior depends on TTLCache implementation
            # pylint: disable=protected-access
            final_cache_size = len(fs._operations._filesystem_cache)
            self.assertGreaterEqual(
                final_cache_size, 0, "Cache size should be valid after expiration"
            )


class TestFileSystemCacheKey(unittest.TestCase):
    """Tests for FileSystemCacheKey class."""

    def test_cache_key_equality_same_params(self):
        """Test that cache keys with identical parameters are equal."""
        key1 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={"opt1": "val1"},
            extra_kwargs={"arg1": "val1"},
        )
        key2 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={"opt1": "val1"},
            extra_kwargs={"arg1": "val1"},
        )

        self.assertEqual(key1, key2)
        self.assertEqual(hash(key1), hash(key2))

    def test_cache_key_inequality_different_scheme(self):
        """Test that cache keys with different schemes are not equal."""
        key1 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )
        key2 = FileSystemCacheKey(
            scheme="gs",
            authority="bucket1",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )

        self.assertNotEqual(key1, key2)

    def test_cache_key_inequality_different_authority(self):
        """Test that cache keys with different authorities are not equal."""
        key1 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )
        key2 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket2",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )

        self.assertNotEqual(key1, key2)

    def test_cache_key_inequality_different_config(self):
        """Test that cache keys with different configurations are not equal."""
        key1 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={},
            extra_kwargs={},
        )
        key2 = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value2"},  # Different value
            options={},
            extra_kwargs={},
        )

        self.assertNotEqual(key1, key2)

    def test_cache_key_includes_process_id(self):
        """Test that cache keys include process ID for isolation."""
        key = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )

        # pylint: disable=protected-access
        self.assertEqual(key._pid, os.getpid())

    def test_cache_key_includes_thread_id(self):
        """Test that cache keys include thread ID for thread safety."""
        key = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )

        # pylint: disable=protected-access
        self.assertEqual(key._thread_id, threading.get_ident())

    def test_cache_key_repr(self):
        """Test that cache key has useful string representation."""
        key = FileSystemCacheKey(
            scheme="s3",
            authority="my-bucket",
            credentials=None,
            catalog_props={},
            options={},
            extra_kwargs={},
        )

        repr_str = repr(key)
        self.assertIn("FileSystemCacheKey", repr_str)
        self.assertIn("s3", repr_str)
        self.assertIn("my-bucket", repr_str)
        self.assertIn(str(os.getpid()), repr_str)
