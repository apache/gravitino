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
from unittest.mock import MagicMock, patch
from urllib.error import URLError

from gravitino.exceptions.base import NotFoundException, RESTException
from gravitino.filesystem.gvfs_default_operations import DefaultGVFSOperations
from gravitino.name_identifier import NameIdentifier


# pylint: disable=protected-access
class TestGVFSMergeSecrets(unittest.TestCase):
    """Unit tests for _merge_fileset_properties including get_secrets()."""

    def _merge_with_catalog(self, operations, catalog):
        schema = MagicMock()
        fileset = MagicMock()
        catalog.as_schemas.return_value.load_schema.return_value = schema
        schema.properties.return_value = {}
        schema.get_secrets.return_value = {}
        catalog.as_fileset_catalog.return_value.load_fileset.return_value = fileset
        fileset.properties.return_value = {}
        fileset.get_secrets.return_value = {}

        client = MagicMock()
        client.load_catalog.return_value = catalog

        with patch.object(operations, "_get_gravitino_client", return_value=client):
            with patch.object(operations, "_get_user_defined_configs", return_value={}):
                return operations._merge_fileset_properties(
                    NameIdentifier.of("ml", "catalog", "schema", "fs"),
                    "s3://bucket/data",
                )

    def test_merge_secrets(self):
        operations = DefaultGVFSOperations(
            server_uri="http://localhost:8090", metalake_name="ml", options={}
        )

        catalog = MagicMock()
        schema = MagicMock()
        fileset = MagicMock()
        catalog.properties.return_value = {"c-vis": "1"}
        catalog.get_secrets.return_value = {"c-secret": "cs"}
        catalog.as_schemas.return_value.load_schema.return_value = schema
        schema.properties.return_value = {"s-vis": "2"}
        schema.get_secrets.return_value = {"s-secret": "ss"}
        catalog.as_fileset_catalog.return_value.load_fileset.return_value = fileset
        fileset.properties.return_value = {"f-vis": "3"}
        fileset.get_secrets.return_value = {"f-secret": "fs"}

        client = MagicMock()
        client.load_catalog.return_value = catalog

        with patch.object(operations, "_get_gravitino_client", return_value=client):
            with patch.object(operations, "_get_user_defined_configs", return_value={}):
                merged = operations._merge_fileset_properties(
                    NameIdentifier.of("ml", "catalog", "schema", "fs"),
                    "file:///tmp/data",
                )

        self.assertEqual(merged["c-vis"], "1")
        self.assertEqual(merged["c-secret"], "cs")
        self.assertEqual(merged["s-vis"], "2")
        self.assertEqual(merged["s-secret"], "ss")
        self.assertEqual(merged["f-vis"], "3")
        self.assertEqual(merged["f-secret"], "fs")

    def test_secret_override(self):
        operations = DefaultGVFSOperations(
            server_uri="http://localhost:8090", metalake_name="ml", options={}
        )

        catalog = MagicMock()
        schema = MagicMock()
        fileset = MagicMock()
        catalog.properties.return_value = {"shared": "from-catalog-props"}
        catalog.get_secrets.return_value = {"shared": "from-catalog-secret"}
        catalog.as_schemas.return_value.load_schema.return_value = schema
        schema.properties.return_value = {"shared": "from-schema-props"}
        schema.get_secrets.return_value = {"shared": "from-schema-secret"}
        catalog.as_fileset_catalog.return_value.load_fileset.return_value = fileset
        fileset.properties.return_value = {"shared": "from-fileset-props"}
        fileset.get_secrets.return_value = {"shared": "from-fileset-secret"}

        client = MagicMock()
        client.load_catalog.return_value = catalog

        with patch.object(operations, "_get_gravitino_client", return_value=client):
            with patch.object(operations, "_get_user_defined_configs", return_value={}):
                merged = operations._merge_fileset_properties(
                    NameIdentifier.of("ml", "catalog", "schema", "fs"),
                    "file:///tmp/data",
                )

        self.assertEqual(merged["shared"], "from-fileset-secret")

    def test_null_props(self):
        operations = DefaultGVFSOperations(
            server_uri="http://localhost:8090", metalake_name="ml", options={}
        )

        catalog = MagicMock()
        schema = MagicMock()
        fileset = MagicMock()
        catalog.properties.return_value = None
        catalog.get_secrets.return_value = {"c-secret": "cs"}
        catalog.as_schemas.return_value.load_schema.return_value = schema
        schema.properties.return_value = None
        schema.get_secrets.return_value = {"s-secret": "ss"}
        catalog.as_fileset_catalog.return_value.load_fileset.return_value = fileset
        fileset.properties.return_value = None
        fileset.get_secrets.return_value = {"f-secret": "fs"}

        client = MagicMock()
        client.load_catalog.return_value = catalog

        with patch.object(operations, "_get_gravitino_client", return_value=client):
            with patch.object(operations, "_get_user_defined_configs", return_value={}):
                merged = operations._merge_fileset_properties(
                    NameIdentifier.of("ml", "catalog", "schema", "fs"),
                    "file:///tmp/data",
                )

        self.assertEqual(merged["c-secret"], "cs")
        self.assertEqual(merged["s-secret"], "ss")
        self.assertEqual(merged["f-secret"], "fs")
        self.assertEqual(len(merged), 3)

    def test_static_catalog_credentials_map_to_gvfs_keys(self):
        operations = DefaultGVFSOperations(
            server_uri="http://localhost:8090", metalake_name="ml", options={}
        )

        catalog = MagicMock()
        schema = MagicMock()
        fileset = MagicMock()
        catalog.name.return_value = "catalog"
        catalog.properties.return_value = {}
        catalog.get_secrets.return_value = {}
        catalog.as_schemas.return_value.load_schema.return_value = schema
        schema.properties.return_value = {}
        schema.get_secrets.return_value = {}
        catalog.as_fileset_catalog.return_value.load_fileset.return_value = fileset
        fileset.properties.return_value = {}
        fileset.get_secrets.return_value = {}

        credential = MagicMock()
        credential.expire_time_in_ms.return_value = 0
        credential.credential_info.return_value = {
            "s3-access-key-id": "AKIATEST",
            "s3-secret-access-key": "secret",
        }
        catalog.support_credentials.return_value.get_credentials.return_value = [
            credential
        ]

        client = MagicMock()
        client.load_catalog.return_value = catalog

        with patch.object(operations, "_get_gravitino_client", return_value=client):
            with patch.object(operations, "_get_user_defined_configs", return_value={}):
                merged = operations._merge_fileset_properties(
                    NameIdentifier.of("ml", "catalog", "schema", "fs"),
                    "s3://bucket/data",
                )

        self.assertEqual(merged["s3-access-key-id"], "AKIATEST")
        self.assertEqual(merged["s3_access_key_id"], "AKIATEST")
        self.assertEqual(merged["s3-secret-access-key"], "secret")
        self.assertEqual(merged["s3_secret_access_key"], "secret")
        catalog.support_credentials.return_value.get_credentials.assert_called_once()

        # No permanent static-credential cache: each merge reloads get_credentials.
        with patch.object(operations, "_get_gravitino_client", return_value=client):
            with patch.object(operations, "_get_user_defined_configs", return_value={}):
                operations._merge_fileset_properties(
                    NameIdentifier.of("ml", "catalog", "schema", "fs"),
                    "s3://bucket/data",
                )
        self.assertEqual(
            catalog.support_credentials.return_value.get_credentials.call_count, 2
        )

    def test_rest_failure_retries_static_credentials_on_next_merge(self):
        operations = DefaultGVFSOperations(
            server_uri="http://localhost:8090", metalake_name="ml", options={}
        )

        catalog = MagicMock()
        catalog.name.return_value = "catalog"
        catalog.properties.return_value = {}
        catalog.get_secrets.return_value = {}

        credential = MagicMock()
        credential.expire_time_in_ms.return_value = 0
        credential.credential_info.return_value = {
            "s3-access-key-id": "AKIATEST",
            "s3-secret-access-key": "secret-after-retry",
        }
        supports = MagicMock()
        supports.get_credentials.side_effect = [
            RESTException("transient failure"),
            [credential],
        ]
        catalog.support_credentials.return_value = supports

        first = self._merge_with_catalog(operations, catalog)
        self.assertNotIn("s3-access-key-id", first)

        second = self._merge_with_catalog(operations, catalog)
        self.assertEqual(second["s3-access-key-id"], "AKIATEST")
        self.assertEqual(second["s3_access_key_id"], "AKIATEST")
        self.assertEqual(supports.get_credentials.call_count, 2)

    def test_get_credentials_failures_do_not_abort_merge(self):
        for exc in (
            NotFoundException("credentials endpoint missing"),
            URLError("connection refused"),
        ):
            with self.subTest(exc=type(exc).__name__):
                operations = DefaultGVFSOperations(
                    server_uri="http://localhost:8090", metalake_name="ml", options={}
                )
                catalog = MagicMock()
                catalog.name.return_value = "catalog"
                catalog.properties.return_value = {
                    "s3-endpoint": "http://s3.example.com"
                }
                catalog.get_secrets.return_value = {}
                catalog.support_credentials.return_value.get_credentials.side_effect = (
                    exc
                )

                merged = self._merge_with_catalog(operations, catalog)
                self.assertEqual(merged["s3-endpoint"], "http://s3.example.com")
                self.assertNotIn("s3-access-key-id", merged)

    def test_skips_expiring_credentials(self):
        operations = DefaultGVFSOperations(
            server_uri="http://localhost:8090", metalake_name="ml", options={}
        )
        catalog = MagicMock()
        catalog.name.return_value = "catalog"
        catalog.properties.return_value = {}
        catalog.get_secrets.return_value = {}

        token = MagicMock()
        token.expire_time_in_ms.return_value = 1_700_000_000_000
        token.credential_info.return_value = {
            "s3-access-key-id": "SESSION",
            "s3-secret-access-key": "session-secret",
            "s3-session-token": "tok",
        }
        static = MagicMock()
        static.expire_time_in_ms.return_value = 0
        static.credential_info.return_value = {
            "s3-access-key-id": "AKIATEST",
            "s3-secret-access-key": "static-secret",
        }
        catalog.support_credentials.return_value.get_credentials.return_value = [
            token,
            static,
        ]

        merged = self._merge_with_catalog(operations, catalog)
        self.assertEqual(merged["s3-access-key-id"], "AKIATEST")
        self.assertEqual(merged["s3_access_key_id"], "AKIATEST")
        self.assertEqual(merged["s3-secret-access-key"], "static-secret")
        self.assertNotIn("s3-session-token", merged)


if __name__ == "__main__":
    unittest.main()
