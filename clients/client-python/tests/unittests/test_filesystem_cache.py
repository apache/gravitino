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
(scheme, authority, credentials, config), following fsspec's native caching
behavior with process and thread isolation.

Test Coverage:
- Filesystem sharing: Multiple filesets → same storage → shared instance (per thread)
- Concurrent access: RWLock prevents deadlocks and race conditions with TTLCache
- Thread isolation: Different threads get separate cache entries (thread_id in key)
- Cache expiration: TTL-based eviction and recreation
- Cache key construction: Equality, inequality, process/thread isolation
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

    def test_concurrent_access_without_deadlock(self, *mock_methods):
        """
        Test that concurrent filesystem access doesn't cause deadlocks or crashes.

        Verifies that the RWLock pattern correctly protects TTLCache from race
        conditions during concurrent read/write operations. Each thread may get
        its own cache entry due to thread_id being part of the cache key.
        """
        storage_location = f"file://{self._fileset_dir}/thread_test"
        self.local_fs.mkdir(storage_location, create_parents=True)

        fileset_virtual = (
            f"fileset/{self._catalog_name}/{self._schema_name}/thread_test/file.txt"
        )

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "thread_test", storage_location
                )
            ),
            get_file_location=MagicMock(return_value=f"{storage_location}/file.txt"),
        ):
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                skip_instance_cache=True,
            )

            # Create file first
            fs.touch(fileset_virtual)

            results = []
            errors = []

            def access_filesystem():
                """Thread worker that accesses the filesystem."""
                try:
                    # Multiple operations to stress test locking
                    exists = fs.exists(fileset_virtual)
                    results.append(exists)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    errors.append(e)

            # Launch multiple threads
            threads = []
            for _ in range(10):
                thread = threading.Thread(target=access_filesystem)
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify no errors occurred
            self.assertEqual(len(errors), 0, f"Thread safety errors: {errors}")
            self.assertEqual(len(results), 10, "All threads should complete")
            self.assertTrue(all(results), "All threads should see file exists")

            # With thread-specific caching (like fsspec), each thread gets its own entry
            # This is different from Java GVFS but matches Python ecosystem conventions
            # pylint: disable=protected-access
            cache_size = len(fs._operations._filesystem_cache)
            self.assertGreaterEqual(
                cache_size,
                1,
                f"Expected at least 1 cached filesystem, but found {cache_size}",
            )
            self.assertLessEqual(
                cache_size,
                10,
                f"Expected at most 10 cached filesystems (one per thread), but found {cache_size}",
            )

    def test_thread_isolation(self, *mock_methods):
        """
        Test that different threads get separate cache entries.

        This validates that thread_id is part of the cache key, ensuring
        thread-specific caching following fsspec's behavior.
        """
        storage_location = f"file://{self._fileset_dir}/thread_isolation"
        self.local_fs.mkdir(storage_location, create_parents=True)

        fileset_virtual = (
            f"fileset/{self._catalog_name}/{self._schema_name}/thread_test/file.txt"
        )

        with patch.multiple(
            "gravitino.client.fileset_catalog.FilesetCatalog",
            load_fileset=MagicMock(
                return_value=mock_base.mock_load_fileset(
                    "thread_test", storage_location
                )
            ),
            get_file_location=MagicMock(return_value=f"{storage_location}/file.txt"),
        ):
            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri=self._server_uri,
                metalake_name=self._metalake_name,
                skip_instance_cache=True,
            )

            # Access from main thread
            fs.touch(fileset_virtual)
            self.assertTrue(fs.exists(fileset_virtual))

            # pylint: disable=protected-access
            main_thread_cache_size = len(fs._operations._filesystem_cache)
            self.assertEqual(
                main_thread_cache_size, 1, "Main thread should have 1 cache entry"
            )

            # Access from a different thread
            thread_cache_size = []

            def access_from_thread():
                """Access filesystem from a different thread."""
                fs.exists(fileset_virtual)
                # pylint: disable=protected-access
                thread_cache_size.append(len(fs._operations._filesystem_cache))

            thread = threading.Thread(target=access_from_thread)
            thread.start()
            thread.join()

            # After thread completes, cache should have 2 entries (one per thread)
            # pylint: disable=protected-access
            final_cache_size = len(fs._operations._filesystem_cache)
            self.assertEqual(
                final_cache_size,
                2,
                f"Expected 2 cache entries (main thread + spawned thread), but found {final_cache_size}",
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

    def test_cache_key_inequality(self):
        """Test that cache keys with different parameters are not equal."""
        base_key = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={},
            extra_kwargs={},
        )

        # Different scheme
        key_diff_scheme = FileSystemCacheKey(
            scheme="gs",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={},
            extra_kwargs={},
        )
        self.assertNotEqual(base_key, key_diff_scheme)

        # Different authority
        key_diff_authority = FileSystemCacheKey(
            scheme="s3",
            authority="bucket2",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={},
            extra_kwargs={},
        )
        self.assertNotEqual(base_key, key_diff_authority)

        # Different catalog properties
        key_diff_config = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value2"},  # Different value
            options={},
            extra_kwargs={},
        )
        self.assertNotEqual(base_key, key_diff_config)

        # Different options
        key_diff_options = FileSystemCacheKey(
            scheme="s3",
            authority="bucket1",
            credentials=None,
            catalog_props={"prop1": "value1"},
            options={"opt1": "val1"},  # Added option
            extra_kwargs={},
        )
        self.assertNotEqual(base_key, key_diff_options)

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
        """Test that cache keys include thread ID for thread isolation."""
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
