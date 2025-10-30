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
from unittest import mock

from gravitino import gvfs


class TestGVFSLazyLoading(unittest.TestCase):
    """
    Unit tests for lazy loading behavior of GravitinoClient in GVFS.

    These tests verify that the GravitinoClient is created lazily on first access
    rather than eagerly during filesystem initialization, improving startup performance
    and reducing resource usage when the filesystem is created but never used.
    """

    def test_client_not_created_during_init(self):
        """
        Test that GravitinoClient is not created during filesystem initialization.

        Verifies the lazy loading behavior - the client should remain None after
        the filesystem is constructed, and only be created when first accessed.
        """
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            skip_instance_cache=True,
        )

        # Client should still be None (not initialized yet)
        # pylint: disable=protected-access
        self.assertIsNone(
            fs._operations._client,
            "GravitinoClient should not be created during filesystem initialization",
        )

    @mock.patch("gravitino.filesystem.gvfs_base_operations.create_client")
    def test_client_created_on_first_access(self, mock_create_client):
        """
        Test that GravitinoClient is created lazily on first access.

        Verifies that the client is initialized when _get_gravitino_client() is called
        for the first time, implementing the lazy initialization pattern.
        """
        # Mock the client creation to avoid network calls
        mock_client = mock.MagicMock()
        mock_create_client.return_value = mock_client

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            skip_instance_cache=True,
        )

        # pylint: disable=protected-access
        # Initially, client should be None
        self.assertIsNone(fs._operations._client)

        # Access the client - should trigger initialization
        client = fs._operations._get_gravitino_client()

        # After access, both the returned client and internal _client should be set
        self.assertIsNotNone(
            client, "Client returned from _get_gravitino_client() should not be None"
        )
        self.assertIsNotNone(
            fs._operations._client,
            "Internal _client field should be initialized after first access",
        )

        # Verify create_client was called exactly once
        mock_create_client.assert_called_once()

    @mock.patch("gravitino.filesystem.gvfs_base_operations.create_client")
    def test_same_client_instance_returned(self, mock_create_client):
        """
        Test that the same GravitinoClient instance is returned on multiple calls.

        Verifies the singleton pattern - multiple calls to _get_gravitino_client()
        should return the exact same object instance, not create new clients.
        """
        # Mock the client creation to avoid network calls
        mock_client = mock.MagicMock()
        mock_create_client.return_value = mock_client

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            skip_instance_cache=True,
        )

        # pylint: disable=protected-access
        # Get client twice
        client1 = fs._operations._get_gravitino_client()
        client2 = fs._operations._get_gravitino_client()

        # Both should be the same instance (use 'is' for identity check)
        self.assertIs(
            client1,
            client2,
            "Multiple calls to _get_gravitino_client() should return the same instance",
        )

        # Also verify it's the same as the internal _client field
        self.assertIs(
            client1,
            fs._operations._client,
            "Returned client should be the same as the internal _client field",
        )

        # Verify create_client was called exactly once (not twice)
        mock_create_client.assert_called_once()
