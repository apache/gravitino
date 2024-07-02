"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest

from gravitino import NameIdentifier
from tests.unittests.mock_base import mock_name_identifier_json


class TestNameIdentifier(unittest.TestCase):
    def test_name_identifier_hash(self):
        name_identifier1: NameIdentifier = NameIdentifier.of(
            "test_metalake", "test_catalog", "test_schema", "test_fileset1"
        )
        name_identifier2: NameIdentifier = NameIdentifier.of(
            "test_metalake", "test_catalog", "test_schema", "test_fileset2"
        )
        identifier_dict = {name_identifier1: "test1", name_identifier2: "test2"}

        self.assertEqual("test1", identifier_dict.get(name_identifier1))
        self.assertNotEqual("test2", identifier_dict.get(name_identifier1))

    def test_from_json_name_identifier(self):

        test_name = "test_name_identifier"
        test_namespace_levels = ["1", "2", "3", "4"]

        json_data = mock_name_identifier_json(test_name, test_namespace_levels)

        name_identifier = NameIdentifier.from_json(json_data, infer_missing=True)

        self.assertEqual(test_name, name_identifier.name())
        self.assertEqual(test_namespace_levels, name_identifier.namespace().levels())
