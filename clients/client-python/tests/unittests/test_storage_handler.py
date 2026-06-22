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
from unittest.mock import patch

from fsspec.implementations.memory import MemoryFileSystem

from gravitino.filesystem.gvfs_storage_handler import (
    ABSStorageHandler,
    GCSStorageHandler,
    OSSStorageHandler,
    S3StorageHandler,
)


class TestStorageHandler(unittest.TestCase):
    def setUp(self):
        # Set up any necessary state before each test
        pass

    def tearDown(self):
        # Clean up after each test
        pass

    def test_s3_storage_handler(self):
        s3_storage_handler = S3StorageHandler()

        # Mock the get_filesystem method to return a mock filesystem
        mock_filesystem = MemoryFileSystem()
        captured_args = {}

        def capture_args_and_return_mock(*args, **kwargs):
            captured_args.update(
                {
                    "key": kwargs.get("key"),
                    "secret": kwargs.get("secret"),
                    "endpoint_url": kwargs.get("endpoint_url"),
                }
            )
            return mock_filesystem

        with patch.object(
            s3_storage_handler,
            "get_filesystem",
            side_effect=capture_args_and_return_mock,
        ):
            result = s3_storage_handler.get_filesystem_with_expiration(
                [],
                {
                    "s3_endpoint": "endpoint_from_client",
                    "s3_access_key_id": "access_key_from_client",
                    "s3_secret_access_key": "secret_key_from_client",
                },
                None,
            )

            self.assertEqual(result[1], mock_filesystem)
            self.assertEqual(captured_args["key"], "access_key_from_client")
            self.assertEqual(captured_args["secret"], "secret_key_from_client")
            self.assertEqual(captured_args["endpoint_url"], "endpoint_from_client")

            captured_args = {}
            result = s3_storage_handler.get_filesystem_with_expiration(
                [],
                {
                    "s3-endpoint": "endpoint_from_catalog",
                    "s3_access_key_id": "access_key_from_client",
                    "s3_secret_access_key": "secret_key_from_client",
                },
                None,
            )

            self.assertEqual(result[1], mock_filesystem)
            self.assertEqual(captured_args["key"], "access_key_from_client")
            self.assertEqual(captured_args["secret"], "secret_key_from_client")
            self.assertEqual(captured_args["endpoint_url"], "endpoint_from_catalog")

    def test_contains_client_credentials(self):
        # S3: both keys required.
        s3_handler = S3StorageHandler()
        self.assertTrue(
            s3_handler.contains_client_credentials(
                {"s3_access_key_id": "ak", "s3_secret_access_key": "sk"}
            )
        )
        self.assertFalse(
            s3_handler.contains_client_credentials({"s3_access_key_id": "ak"})
        )
        self.assertFalse(
            s3_handler.contains_client_credentials({"s3_endpoint": "endpoint"})
        )

        # OSS: both keys required.
        oss_handler = OSSStorageHandler()
        self.assertTrue(
            oss_handler.contains_client_credentials(
                {"oss_access_key_id": "ak", "oss_secret_access_key": "sk"}
            )
        )
        self.assertFalse(
            oss_handler.contains_client_credentials({"oss_access_key_id": "ak"})
        )

        # GCS: service account file.
        gcs_handler = GCSStorageHandler()
        self.assertTrue(
            gcs_handler.contains_client_credentials(
                {"gcs_service_account_file": "/path/to/file"}
            )
        )
        self.assertFalse(gcs_handler.contains_client_credentials({"other": "value"}))

        # ABS: account name + key.
        abs_handler = ABSStorageHandler()
        self.assertTrue(
            abs_handler.contains_client_credentials(
                {
                    "azure_storage_account_name": "name",
                    "azure_storage_account_key": "key",
                }
            )
        )
        self.assertFalse(
            abs_handler.contains_client_credentials(
                {"azure_storage_account_name": "name"}
            )
        )
