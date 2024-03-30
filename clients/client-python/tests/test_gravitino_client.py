"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import unittest
from unittest.mock import patch

from gravitino import GravitinoClient, gravitino_metalake, MetaLake


@patch('gravitino.service._Service.get_version', return_value={'foo': 'bar'})
class TestGravitinoClient(unittest.TestCase):
    def setUp(self):
        self.client = GravitinoClient("http://localhost:8090")

    def test_gravitino_version(self, *args):
        self.assertEquals(self.client.version, {'foo': 'bar'})
