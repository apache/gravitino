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
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.exceptions.base import IllegalArgumentException


class MockPolicyContent(PolicyContent):
    """Concrete implementation for testing."""

    def __init__(self, object_types=None, props=None):
        self._object_types = object_types or set()
        self._props = props or {}

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return self._object_types

    def properties(self) -> dict[str, str]:
        return self._props


class TestPolicyContent(unittest.TestCase):
    def test_supported_object_types(self):
        object_types = {MetadataObject.Type.CATALOG, MetadataObject.Type.FILESET}
        policy = MockPolicyContent(object_types=object_types)
        self.assertSetEqual(policy.supported_object_types(), object_types)

    def test_properties(self):
        props = {"key1": "value1", "key2": "value2"}
        policy = MockPolicyContent(props=props)
        self.assertDictEqual(policy.properties(), props)

    def test_validate_with_empty_object_types_raises_error(self):
        policy = MockPolicyContent(object_types=set())
        with self.assertRaisesRegex(
            IllegalArgumentException, "supported_object_types cannot be empty"
        ):
            policy.validate()

    def test_validate_with_non_empty_object_types_passes(self):
        policy = MockPolicyContent(object_types={MetadataObject.Type.CATALOG})
        policy.validate()
