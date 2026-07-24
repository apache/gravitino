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
import asyncio
import json
import unittest

from mcp_server.tools.metadata import _find_table_metadata


class _FakeCatalogOperation:
    def __init__(self, catalogs):
        self._catalogs = catalogs

    async def get_list_of_catalogs(self):
        return json.dumps(self._catalogs)


class _FakeSchemaOperation:
    def __init__(self, schemas_by_catalog):
        self._schemas_by_catalog = schemas_by_catalog

    async def get_list_of_schemas(self, catalog_name):
        entry = self._schemas_by_catalog[catalog_name]
        if isinstance(entry, Exception):
            raise entry
        return json.dumps(entry)


class _FakeTableOperation:
    def __init__(self, tables_by_schema):
        self._tables_by_schema = tables_by_schema

    async def get_list_of_tables(self, catalog_name, schema_name):
        entry = self._tables_by_schema[(catalog_name, schema_name)]
        if isinstance(entry, Exception):
            raise entry
        return json.dumps(entry)


class _FakeClient:
    """Minimal stand-in exposing only the three operations the crawl uses."""

    def __init__(self, catalogs, schemas_by_catalog, tables_by_schema):
        self._catalog_operation = _FakeCatalogOperation(catalogs)
        self._schema_operation = _FakeSchemaOperation(schemas_by_catalog)
        self._table_operation = _FakeTableOperation(tables_by_schema)

    def as_catalog_operation(self):
        return self._catalog_operation

    def as_schema_operation(self):
        return self._schema_operation

    def as_table_operation(self):
        return self._table_operation


def _sample_client():
    """A metalake with two relational catalogs, one fileset catalog to be
    ignored, and one schema that fails to list (managed by multiple catalogs).
    """
    catalogs = [
        {"name": "catalog_postgres", "type": "relational"},
        {"name": "catalog_hive", "type": "relational"},
        {"name": "catalog_fileset", "type": "fileset"},
    ]
    schemas_by_catalog = {
        "catalog_postgres": [{"name": "hr"}, {"name": "public"}],
        "catalog_hive": [{"name": "product"}, {"name": "sales"}],
    }
    tables_by_schema = {
        ("catalog_postgres", "hr"): [{"name": "employees"}, {"name": "salaries"}],
        ("catalog_postgres", "public"): [],
        ("catalog_hive", "product"): [{"name": "Employees"}],
        ("catalog_hive", "sales"): Exception(
            "Schema managed by multiple catalogs"
        ),
    }
    return _FakeClient(catalogs, schemas_by_catalog, tables_by_schema)


class TestFindTableMetadata(unittest.TestCase):
    """Unit tests for the find_metadata crawl helper."""

    def test_case_insensitive_match_across_catalogs(self):
        """A case-insensitive search returns hits from every relational
        catalog, including a name that differs only by case."""
        result = asyncio.run(
            _find_table_metadata(_sample_client(), "employees", False)
        )
        full_names = {match["fullName"] for match in result["matches"]}
        self.assertEqual(
            full_names,
            {
                "catalog_postgres.hr.employees",
                "catalog_hive.product.Employees",
            },
        )

    def test_fileset_catalog_is_ignored(self):
        """Only relational catalogs are crawled; the fileset catalog is not
        searched and does not appear in the counts."""
        result = asyncio.run(
            _find_table_metadata(_sample_client(), "employees", False)
        )
        self.assertEqual(result["searched"]["catalogs"], 2)
        self.assertEqual(result["searched"]["schemas"], 4)

    def test_unlistable_schema_is_skipped_not_fatal(self):
        """A schema that cannot be listed is recorded under 'skipped' rather
        than aborting the crawl."""
        result = asyncio.run(
            _find_table_metadata(_sample_client(), "employees", False)
        )
        self.assertEqual(
            result["skipped"],
            [
                {
                    "location": "catalog_hive.sales",
                    "reason": "Schema managed by multiple catalogs",
                }
            ],
        )

    def test_case_sensitive_excludes_differing_case(self):
        """A case-sensitive search drops a name that differs only by case."""
        result = asyncio.run(
            _find_table_metadata(_sample_client(), "employees", True)
        )
        full_names = {match["fullName"] for match in result["matches"]}
        self.assertEqual(full_names, {"catalog_postgres.hr.employees"})

    def test_substring_match(self):
        """Matching is a substring match by default."""
        result = asyncio.run(
            _find_table_metadata(_sample_client(), "ployee", False)
        )
        full_names = {match["fullName"] for match in result["matches"]}
        self.assertEqual(
            full_names,
            {
                "catalog_postgres.hr.employees",
                "catalog_hive.product.Employees",
            },
        )

    def test_no_match_returns_empty(self):
        """A name that matches nothing returns no matches but still reports
        what was searched."""
        result = asyncio.run(
            _find_table_metadata(_sample_client(), "no_such_table", False)
        )
        self.assertEqual(result["matches"], [])
        self.assertEqual(result["searched"]["catalogs"], 2)

    def test_empty_name_is_rejected(self):
        """A blank name raises rather than matching every table."""
        with self.assertRaises(ValueError):
            asyncio.run(_find_table_metadata(_sample_client(), "  ", False))


if __name__ == "__main__":
    unittest.main()
