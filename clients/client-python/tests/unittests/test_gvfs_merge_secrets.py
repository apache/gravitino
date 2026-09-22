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

from gravitino.filesystem.gvfs_default_operations import DefaultGVFSOperations
from gravitino.name_identifier import NameIdentifier


# pylint: disable=protected-access
class TestGVFSMergeSecrets(unittest.TestCase):
    """Unit tests for _merge_fileset_properties including get_secrets()."""

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


if __name__ == "__main__":
    unittest.main()
