"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest

from gravitino import NameIdentifier


class TestNameIdentifier(unittest.TestCase):
    def test_name_identifier_hash(self):
        name_identifier1: NameIdentifier = NameIdentifier.of_fileset(
            "test_metalake", "test_catalog", "test_schema", "test_fileset1"
        )
        name_identifier2: NameIdentifier = NameIdentifier.of_fileset(
            "test_metalake", "test_catalog", "test_schema", "test_fileset2"
        )
        identifier_dict = {name_identifier1: "test1", name_identifier2: "test2"}

        self.assertEqual("test1", identifier_dict.get(name_identifier1))
        self.assertNotEqual("test2", identifier_dict.get(name_identifier1))
