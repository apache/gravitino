"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest
from unittest.mock import patch

from gravitino import GravitinoClient, gravitino_metalake
from . import fixtures


@patch("gravitino.service._Service.get_version", return_value=fixtures.services_version)
@patch(
    "gravitino.service._Service.list_metalakes",
    return_value=fixtures.services_list_metalakes,
)
@patch(
    "gravitino.service._Service.get_metalake",
    return_value=fixtures.services_get_metalake,
)
@patch(
    "gravitino.service._Service.list_catalogs",
    return_value=fixtures.services_list_catalogs,
)
@patch(
    "gravitino.service._Service.get_catalog",
    return_value=fixtures.services_get_catalog,
)
@patch(
    "gravitino.service._Service.list_schemas",
    return_value=fixtures.services_list_schemas,
)
@patch(
    "gravitino.service._Service.get_schema",
    return_value=fixtures.services_get_schema,
)
@patch(
    "gravitino.service._Service.list_tables",
    return_value=fixtures.services_list_tables,
)
@patch(
    "gravitino.service._Service.get_table",
    return_value=fixtures.services_get_table,
)
class TestGravitinoClient(unittest.TestCase):
    def setUp(self):
        self.client = GravitinoClient("http://localhost:9000")

    def test_version(self, *args):
        self.assertIn("version", list(self.client.version.keys()))

    def test_get_metalakes(self, *args):
        metalakes = self.client.get_metalakes()
        self.assertEqual(len(metalakes), 1)
        self.assertEqual(metalakes[0].name, "metalake_demo")

    def test_get_metalake(self, *args):
        metalake = self.client.get_metalake("metalake_demo")
        self.assertEqual(metalake.name, "metalake_demo")

    def test_dynamic_properties(self, *args):
        metalake = self.client.get_metalake("metalake_demo")
        self.assertIn("catalog_hive", dir(metalake))
        self.assertIn("catalog_iceberg", dir(metalake))
        self.assertIn("catalog_postgres", dir(metalake))
        self.assertEqual(metalake.catalog_hive.name, "catalog_hive")
        self.assertEqual(metalake.catalog_hive.sales.name, "sales")
        self.assertEqual(metalake.catalog_hive.sales.sales.info().get("name"), "sales")


@patch(
    "gravitino.service._Service.get_metalake",
    return_value=fixtures.services_get_metalake,
)
@patch(
    "gravitino.service._Service.list_catalogs",
    return_value=fixtures.services_list_catalogs,
)
@patch(
    "gravitino.service._Service.get_catalog",
    return_value=fixtures.services_get_catalog,
)
class TestGravitinoMetalake(unittest.TestCase):
    def test_gravitino_metalake(self, *args):
        metalake = gravitino_metalake("http://localhost:9000", "metalake_demo")
        self.assertEqual(metalake.name, "metalake_demo")
