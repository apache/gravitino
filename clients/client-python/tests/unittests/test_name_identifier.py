"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

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
