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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_contents import PolicyContents


class TestPolicyContents(unittest.TestCase):
    def test_custom_creates_custom_content(self):
        rules = {"rule1": "value1"}
        object_types = {MetadataObject.Type.CATALOG}
        properties = {"prop1": "val1"}

        content = PolicyContents.custom(rules, object_types, properties)

        self.assertIsInstance(content, PolicyContents.CustomContent)
        self.assertEqual(content.custom_rules(), rules)
        self.assertEqual(content.supported_object_types(), object_types)
        self.assertEqual(content.properties(), properties)

    def test_custom_content_init_with_none_values(self):
        content = PolicyContents.CustomContent()

        self.assertIsNone(content.custom_rules())
        self.assertSetEqual(content.supported_object_types(), set())
        self.assertIsNone(content.properties())

    def test_custom_content_init_with_values(self):
        rules = {"rule1": "value1"}
        object_types = {MetadataObject.Type.CATALOG, MetadataObject.Type.FILESET}
        properties = {"prop1": "val1"}

        content = PolicyContents.CustomContent(rules, object_types, properties)

        self.assertEqual(content.custom_rules(), rules)
        self.assertSetEqual(content.supported_object_types(), object_types)
        self.assertEqual(content.properties(), properties)

    def test_equality_same_objects(self):
        rules = {"rule1": "value1"}
        object_types = {MetadataObject.Type.CATALOG}
        properties = {"prop1": "val1"}

        content1 = PolicyContents.custom(rules, object_types, properties)
        content2 = PolicyContents.custom(rules, object_types, properties)

        self.assertEqual(content1, content2)

    def test_equality_different_rules(self):
        object_types = {MetadataObject.Type.CATALOG}
        properties = {"prop1": "val1"}

        content1 = PolicyContents.CustomContent(
            {"rule1": "value1"}, object_types, properties
        )
        content2 = PolicyContents.CustomContent(
            {"rule2": "value2"}, object_types, properties
        )

        self.assertNotEqual(content1, content2)

    def test_equality_different_types(self):
        content = PolicyContents.CustomContent()

        self.assertNotEqual(content, "not a CustomContent")
        self.assertNotEqual(content, None)

    def test_hash_consistency(self):
        rules = {"rule1": "value1"}
        object_types = {MetadataObject.Type.CATALOG}
        properties = {"prop1": "val1"}

        content1 = PolicyContents.CustomContent(rules, object_types, properties)
        content2 = PolicyContents.CustomContent(rules, object_types, properties)

        self.assertEqual(hash(content1), hash(content2))

    def test_str_representation(self):
        rules = {"rule1": "value1"}
        object_types = {MetadataObject.Type.CATALOG}
        properties = {"prop1": "val1"}

        content = PolicyContents.CustomContent(rules, object_types, properties)
        str_repr = str(content)

        self.assertIn("CustomContent", str_repr)
        self.assertIn("custom_rules", str_repr)
        self.assertIn("properties", str_repr)
        self.assertIn("supported_object_types", str_repr)
