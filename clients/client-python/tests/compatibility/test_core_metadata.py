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

"""Acceptance tests for an installed core SDK and a matching real server."""

import importlib.metadata
import os
from pathlib import Path
import sys
import unittest
from uuid import uuid4

import gravitino
from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, NameIdentifier
from gravitino.api.rel.column import Column
from gravitino.api.rel.types.types import Types
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.exceptions.base import NoSuchSchemaException, NoSuchTableException
from gravitino.namespace import Namespace


class TestInstalledCoreMetadata(unittest.TestCase):
    """Exercise real catalog/schema/table calls with version checking enabled."""

    @classmethod
    def setUpClass(cls):
        sdk_path = Path(gravitino.__file__).resolve()
        if not sdk_path.is_relative_to(Path(sys.prefix).resolve()):
            raise ValueError("Core compatibility tests require an installed SDK")
        source = os.environ.get("GRAVITINO_CORE_COMPATIBILITY_SOURCE")
        if source and sdk_path.is_relative_to((Path(source) / "gravitino").resolve()):
            raise ValueError("SDK checkout must not be imported")
        if (
            os.environ.get(GravitinoClientBase.VERSION_CHECK_DISABLED_ENV, "")
            .strip()
            .lower()
            == "true"
        ):
            raise ValueError("Server version checking must remain enabled")
        uri = os.environ["GRAVITINO_CORE_COMPATIBILITY_URI"]
        cls.metalake = f"core_compat_{uuid4().hex}"
        cls.admin = GravitinoAdminClient(uri=uri)
        cls.addClassCleanup(cls.admin.close)
        cls.admin.create_metalake(
            cls.metalake, comment="Core SDK acceptance", properties={}
        )
        cls.addClassCleanup(cls.admin.drop_metalake, cls.metalake, True)
        cls.client = GravitinoClient(uri=uri, metalake_name=cls.metalake)
        cls.addClassCleanup(cls.client.close)
        cls.catalog = cls.client.create_catalog(
            name="metadata",
            catalog_type=Catalog.Type.RELATIONAL,
            provider="lakehouse-generic",
            comment="Core metadata compatibility",
            properties={},
        )
        cls.catalog.as_schemas().create_schema("schema", "Core schema", {})
        cls.identifier = NameIdentifier.of("schema", "table")
        cls.catalog.as_table_catalog().create_table(
            identifier=cls.identifier,
            columns=[Column.of("id", Types.IntegerType.get())],
            comment="External metadata fixture",
            properties={
                "format": "delta",
                "external": "true",
                "location": f"file:///tmp/{cls.metalake}/table",
            },
        )

    def test_installed_distribution_contract(self):
        """The installed distribution retains only the declared core dependencies."""
        distribution = importlib.metadata.distribution("apache-gravitino")
        core = [value for value in distribution.requires or [] if ";" not in value]
        self.assertEqual(len(core), 2)
        self.assertTrue(any(value.startswith("dataclasses-json") for value in core))
        simplejson = next(value for value in core if value.startswith("simplejson"))
        self.assertIn(">=3.20.1", simplejson)
        self.assertIn("<=4.2.0", simplejson)

    def test_catalog_schema_table_round_trip(self):
        """Real metadata retains names, columns, properties and logical identifiers."""
        names = self.client.list_catalogs()
        self.assertIn("metadata", names)
        catalog = self.client.load_catalog("metadata")
        schema = catalog.as_schemas().load_schema("schema")
        self.assertEqual(schema.name(), "schema")
        tables = catalog.as_table_catalog()
        self.assertIn(self.identifier, tables.list_tables(Namespace.of("schema")))
        table = tables.load_table(self.identifier)
        self.assertEqual(table.name(), "table")
        self.assertEqual(table.columns()[0].name(), "id")
        self.assertEqual(table.columns()[0].data_type(), Types.IntegerType.get())
        self.assertEqual(table.properties()["external"], "true")

    def test_absent_objects_preserve_sdk_exceptions(self):
        """Actual error responses map to official SDK exception types."""
        with self.assertRaises(NoSuchSchemaException):
            self.catalog.as_schemas().load_schema("missing")
        with self.assertRaises(NoSuchTableException):
            self.catalog.as_table_catalog().load_table(
                NameIdentifier.of("schema", "missing")
            )


if __name__ == "__main__":
    unittest.main()
