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

import re
import unittest
from unittest.mock import MagicMock, patch

from fsspec.implementations.memory import MemoryFileSystem

from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.filesystem.gvfs_storage_handler import (
    ABSStorageHandler,
    GCSStorageHandler,
    HDFSStorageHandler,
    OSSStorageHandler,
    S3StorageHandler,
)

STORAGE_IMPORT_MODULE = (
    "gravitino.filesystem.gvfs_storage_handler.importlib.import_module"
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

    def test_missing_provider_dependency_has_install_guidance(self):
        handlers = [
            (HDFSStorageHandler(), "pyarrow", "hdfs"),
            (S3StorageHandler(), "s3fs", "s3"),
            (GCSStorageHandler(), "gcsfs", "gcs"),
            (OSSStorageHandler(), "ossfs", "oss"),
            (ABSStorageHandler(), "adlfs", "azure"),
        ]

        for handler, missing_module, extra_name in handlers:
            with self.subTest(extra=extra_name):
                with patch(
                    STORAGE_IMPORT_MODULE,
                    side_effect=ModuleNotFoundError(
                        f"No module named '{missing_module}'",
                        name=missing_module,
                    ),
                ):
                    with self.assertRaisesRegex(
                        GravitinoRuntimeException,
                        re.escape(f"apache-gravitino[{extra_name}]"),
                    ):
                        handler.get_filesystem("unused://path")

    def test_provider_handlers_dispatch_to_their_filesystem_class(self):
        handlers = [
            (HDFSStorageHandler(), "pyarrow.fs", "HadoopFileSystem"),
            (S3StorageHandler(), "s3fs", "S3FileSystem"),
            (GCSStorageHandler(), "gcsfs", "GCSFileSystem"),
            (OSSStorageHandler(), "ossfs", "OSSFileSystem"),
            (ABSStorageHandler(), "adlfs", "AzureBlobFileSystem"),
        ]

        for handler, module_name, class_name in handlers:
            with self.subTest(provider=module_name):
                provider_module = MagicMock()
                filesystem_class = getattr(provider_module, class_name)
                filesystem = MagicMock()

                if module_name == "pyarrow.fs":
                    filesystem_class.from_uri.return_value = filesystem
                    with (
                        patch(
                            "gravitino.filesystem.gvfs_storage_handler.ArrowFSWrapper",
                            return_value=filesystem,
                        ) as arrow_wrapper,
                        patch(
                            STORAGE_IMPORT_MODULE, return_value=provider_module
                        ) as import_module,
                    ):
                        result = handler.get_filesystem("hdfs://namenode:8020/path")

                    filesystem_class.from_uri.assert_called_once_with(
                        "hdfs://namenode:8020/path"
                    )
                    arrow_wrapper.assert_called_once_with(filesystem)
                else:
                    filesystem_class.return_value = filesystem
                    with patch(
                        STORAGE_IMPORT_MODULE, return_value=provider_module
                    ) as import_module:
                        result = handler.get_filesystem(
                            "unused://path", test_option="value"
                        )

                    filesystem_class.assert_called_once_with(test_option="value")

                import_module.assert_called_once_with(module_name)
                self.assertIs(result, filesystem)

    def test_provider_transitive_import_error_is_preserved(self):
        handler = S3StorageHandler()
        with patch(
            STORAGE_IMPORT_MODULE,
            side_effect=ModuleNotFoundError(
                "No module named 'botocore'", name="botocore"
            ),
        ):
            with self.assertRaisesRegex(ModuleNotFoundError, "botocore"):
                handler.get_filesystem("s3a://bucket/path")
