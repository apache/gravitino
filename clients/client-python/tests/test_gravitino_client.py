"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest

from gravitino.gravitino_client import gravitino_metalake, GravitinoClient
from .utils import services_fixtures


@services_fixtures
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
        self.assertIn("catalog_hive", metalake)

    def test_get_catalog(self, *args):
        catalog = self.client.get_metalake("metalake_demo").catalog_hive
        self.assertEqual(catalog.name, "catalog_hive")
        self.assertIn("sales", catalog)

    def test_get_schema(self, *args):
        schema = self.client.get_metalake("metalake_demo").catalog_hive.sales
        self.assertEqual(schema.name, "sales")
        self.assertIn("sales", schema)

    def test_get_table(self, *args):
        table = self.client.get_metalake("metalake_demo").catalog_hive.sales.sales
        self.assertEqual(table.name, "sales")
        self.assertEqual(table.info().get("name"), "sales")

    def test_dynamic_properties(self, *args):
        metalake = self.client.get_metalake("metalake_demo")
        self.assertIn("catalog_hive", dir(metalake))
        self.assertIn("catalog_iceberg", dir(metalake))
        self.assertIn("catalog_postgres", dir(metalake))
        self.assertEqual(metalake.catalog_hive.name, "catalog_hive")
        self.assertEqual(metalake.catalog_hive.sales.name, "sales")


@services_fixtures
class TestGravitinoMetalake(unittest.TestCase):
    def test_gravitino_metalake(self, *args):
        metalake = gravitino_metalake("http://localhost:9000", "metalake_demo")
        self.assertEqual(metalake.name, "metalake_demo")
