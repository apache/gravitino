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
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock

from gravitino.api.credential.s3_token_credential import S3TokenCredential
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.name_identifier import NameIdentifier


def _create_mock_credential(expire_time_in_ms):
    """Create a mock S3TokenCredential with the given expire time."""
    return S3TokenCredential(
        credential_info={
            S3TokenCredential._SESSION_ACCESS_KEY_ID: "access_id",
            S3TokenCredential._SESSION_SECRET_ACCESS_KEY: "secret_key",
            S3TokenCredential._SESSION_TOKEN: "session_token",
        },
        expire_time_in_ms=expire_time_in_ms,
    )


def _create_mock_fileset(credentials_list):
    """Create a mock fileset that returns the given credentials."""
    from gravitino.client.generic_fileset import GenericFileset

    mock_support = MagicMock()
    mock_support.get_credentials.return_value = credentials_list
    mock_fileset = MagicMock(spec=GenericFileset)
    mock_fileset.support_credentials.return_value = mock_support
    return mock_fileset


class _ConcreteGVFSOperations:
    """Minimal concrete subclass to test BaseGVFSOperations credential cache logic."""

    pass


class TestGVFSCredentialCache(unittest.TestCase):
    """Tests for credential-level lazy caching in BaseGVFSOperations."""

    def _create_operations(self, options=None):
        """Create a BaseGVFSOperations-like object with credential cache fields."""
        # We directly construct the relevant fields from BaseGVFSOperations.__init__
        # without going through the full constructor to avoid needing all dependencies.
        from readerwriterlock import rwlock

        ops = MagicMock()
        ops._options = options or {}
        ops._enable_credential_vending = (options or {}).get(
            GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING, False
        )
        ops._credential_cache = {}
        ops._credential_cache_lock = rwlock.RWLockFair()

        # Bind the real methods from BaseGVFSOperations
        from gravitino.filesystem.gvfs_base_operations import BaseGVFSOperations

        ops._file_system_expired = BaseGVFSOperations._file_system_expired.__get__(ops)
        ops._calculate_credential_expire_time = (
            BaseGVFSOperations._calculate_credential_expire_time.__get__(ops)
        )
        ops._get_credentials_with_cache = (
            BaseGVFSOperations._get_credentials_with_cache.__get__(ops)
        )
        return ops

    def test_credential_vending_disabled_returns_none(self):
        """When credential vending is disabled, should return None without touching cache."""
        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: False}
        )
        fileset = _create_mock_fileset([])

        result = ops._get_credentials_with_cache(
            NameIdentifier.of("metalake", "catalog", "schema", "fileset"),
            fileset,
            "default",
        )

        self.assertIsNone(result)
        fileset.support_credentials().get_credentials.assert_not_called()

    def test_first_call_fetches_from_server(self):
        """First call should fetch credentials from server and cache them."""
        credential = _create_mock_credential(
            int(time.time() * 1000) + 3600_000
        )  # 1 hour from now
        fileset = _create_mock_fileset([credential])
        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        fileset_ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset")

        result = ops._get_credentials_with_cache(fileset_ident, fileset, "default")

        self.assertEqual(result, [credential])
        fileset.support_credentials().get_credentials.assert_called_once()

    def test_cached_credentials_returned_without_server_call(self):
        """Second call with valid cache should not hit the server."""
        credential = _create_mock_credential(
            int(time.time() * 1000) + 3600_000
        )  # 1 hour from now
        fileset = _create_mock_fileset([credential])
        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        fileset_ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset")

        # First call - fetches from server
        result1 = ops._get_credentials_with_cache(fileset_ident, fileset, "default")
        # Second call - should use cache
        result2 = ops._get_credentials_with_cache(fileset_ident, fileset, "default")

        self.assertEqual(result1, result2)
        # Server should only be called once
        fileset.support_credentials().get_credentials.assert_called_once()

    def test_expired_credentials_trigger_refresh(self):
        """When cached credentials are expired, should fetch new ones from server."""
        from gravitino.client.generic_fileset import GenericFileset

        # Create credential that's already expired (expire time in the past)
        expired_credential = _create_mock_credential(
            int(time.time() * 1000) - 1000
        )  # 1 second ago

        # New credential with future expiry
        fresh_credential = _create_mock_credential(int(time.time() * 1000) + 3600_000)

        # Use a single mock fileset with side_effect to return different values
        mock_support = MagicMock()
        mock_support.get_credentials.side_effect = [
            [expired_credential],
            [fresh_credential],
        ]
        fileset = MagicMock(spec=GenericFileset)
        fileset.support_credentials.return_value = mock_support

        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        fileset_ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset")

        # First call - fetches expired credential from server and caches it
        result1 = ops._get_credentials_with_cache(fileset_ident, fileset, "default")
        # Second call - cache is expired, calls get_credentials() again
        result2 = ops._get_credentials_with_cache(fileset_ident, fileset, "default")

        self.assertEqual(result1, [expired_credential])
        self.assertEqual(result2, [fresh_credential])
        # get_credentials() should have been called twice (first: no cache, second: expired)
        self.assertEqual(
            mock_support.get_credentials.call_count,
            2,
        )

    def test_different_locations_cached_separately(self):
        """Different location names should have separate cache entries."""
        credential_s3 = _create_mock_credential(int(time.time() * 1000) + 3600_000)
        credential_oss = _create_mock_credential(int(time.time() * 1000) + 7200_000)

        fileset_s3 = _create_mock_fileset([credential_s3])
        fileset_oss = _create_mock_fileset([credential_oss])

        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        fileset_ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset")

        result_s3 = ops._get_credentials_with_cache(
            fileset_ident, fileset_s3, "s3-location"
        )
        result_oss = ops._get_credentials_with_cache(
            fileset_ident, fileset_oss, "oss-location"
        )

        self.assertEqual(result_s3, [credential_s3])
        self.assertEqual(result_oss, [credential_oss])
        fileset_s3.support_credentials().get_credentials.assert_called_once()
        fileset_oss.support_credentials().get_credentials.assert_called_once()

    def test_different_filesets_cached_separately(self):
        """Different fileset identifiers should have separate cache entries."""
        credential1 = _create_mock_credential(int(time.time() * 1000) + 3600_000)
        credential2 = _create_mock_credential(int(time.time() * 1000) + 7200_000)

        fileset1 = _create_mock_fileset([credential1])
        fileset2 = _create_mock_fileset([credential2])

        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        ident1 = NameIdentifier.of("metalake", "catalog", "schema", "fileset1")
        ident2 = NameIdentifier.of("metalake", "catalog", "schema", "fileset2")

        result1 = ops._get_credentials_with_cache(ident1, fileset1, "default")
        result2 = ops._get_credentials_with_cache(ident2, fileset2, "default")

        self.assertEqual(result1, [credential1])
        self.assertEqual(result2, [credential2])
        fileset1.support_credentials().get_credentials.assert_called_once()
        fileset2.support_credentials().get_credentials.assert_called_once()

    def test_empty_credentials_cached(self):
        """Empty credential list should be cached to avoid repeated calls."""
        fileset = _create_mock_fileset([])
        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        fileset_ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset")

        result1 = ops._get_credentials_with_cache(fileset_ident, fileset, "default")
        result2 = ops._get_credentials_with_cache(fileset_ident, fileset, "default")

        self.assertEqual(result1, [])
        self.assertEqual(result2, [])
        # Only called once - second call hits cache
        fileset.support_credentials().get_credentials.assert_called_once()

    def test_credential_expire_time_ratio(self):
        """Verify that the credential expiration ratio is applied correctly."""
        ops = self._create_operations(
            {
                GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True,
                GVFSConfig.GVFS_FILESYSTEM_CREDENTIAL_EXPIRED_TIME_RATIO: "0.5",
            }
        )

        # Credential expires in 1000ms
        expire_ms = int(time.time() * 1000) + 1000
        calculated = ops._calculate_credential_expire_time(expire_ms)

        # With ratio 0.5, the cache should expire at roughly current_time + 500ms
        expected_approx = int(time.time() * 1000) + 500
        # Allow 50ms tolerance for test execution time
        self.assertAlmostEqual(calculated, expected_approx, delta=50)

    def test_credential_expire_time_never_expire(self):
        """When credential expire_time_in_ms <= 0, cache should never expire."""
        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )

        result = ops._calculate_credential_expire_time(0)
        self.assertEqual(result, sys.maxsize)

        result = ops._calculate_credential_expire_time(-1)
        self.assertEqual(result, sys.maxsize)

    def test_thread_safety_concurrent_access(self):
        """Multiple threads accessing cache simultaneously should be safe and deduplicated."""
        credential = _create_mock_credential(int(time.time() * 1000) + 3600_000)

        # Use a shared mock fileset so call count is meaningful
        shared_fileset = _create_mock_fileset([credential])

        ops = self._create_operations(
            {GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING: True}
        )
        fileset_ident = NameIdentifier.of("metalake", "catalog", "schema", "fileset")

        num_threads = 10
        results = [None] * num_threads
        errors = [None] * num_threads

        def worker(idx):
            try:
                results[idx] = ops._get_credentials_with_cache(
                    fileset_ident, shared_fileset, "default"
                )
            except Exception as e:
                errors[idx] = e

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No errors should have occurred
        for e in errors:
            self.assertIsNone(e, f"Thread encountered error: {e}")

        # All results should be the same credential list
        for result in results:
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            # 显式类型缩小：在访问索引之前确保 result 不为 None
            assert result is not None  # type: ignore[unreachable]
            self.assertEqual(result[0].access_key_id(), "access_id")

        # Double-check locking should ensure only 1 server call
        shared_fileset.support_credentials().get_credentials.assert_called_once()


if __name__ == "__main__":
    unittest.main()
