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
from unittest.mock import Mock

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.exceptions.base import IllegalArgumentException


class TestPolicyBuiltInType(unittest.TestCase):
    """Test cases for Policy.BuiltInType enum."""

    def test_custom_type_properties(self):
        """Test CUSTOM type properties."""
        custom_type = Policy.BuiltInType.CUSTOM
        self.assertEqual("custom", custom_type.policy_type())
        self.assertEqual(PolicyContents.CustomContent, custom_type.content_class())

    def test_from_policy_type_custom(self):
        """Test from_policy_type with custom type."""
        result = Policy.BuiltInType.from_policy_type("custom")
        self.assertEqual(Policy.BuiltInType.CUSTOM, result)

    def test_from_policy_type_unknown_builtin(self):
        """Test from_policy_type with unknown built-in type."""
        with self.assertRaisesRegex(
            IllegalArgumentException, "Unknown built-in policy type"
        ):
            Policy.BuiltInType.from_policy_type("system_unknown")
        with self.assertRaisesRegex(IllegalArgumentException, "Unknown policy type"):
            Policy.BuiltInType.from_policy_type("invalid_type")

    def test_from_policy_type_empty_string(self):
        """Test from_policy_type with empty string."""
        with self.assertRaisesRegex(
            IllegalArgumentException, "policy_type cannot be blank"
        ):
            Policy.BuiltInType.from_policy_type("")
        with self.assertRaisesRegex(
            IllegalArgumentException, "policy_type cannot be blank"
        ):
            Policy.BuiltInType.from_policy_type(None)


class TestAssociatedObjects(Policy.AssociatedObjects):
    def __init__(self, objects_list):
        self._objects = objects_list

    def objects(self):
        return self._objects


class TestPolicyAssociatedObjects(unittest.TestCase):
    """Test cases for Policy.AssociatedObjects class."""

    def test_count_with_objects(self):
        """Test count method with objects."""
        mock_objects = [Mock(spec=MetadataObject), Mock(spec=MetadataObject)]
        associated = TestAssociatedObjects(mock_objects)
        self.assertEqual(2, associated.count())

    def test_count_with_empty_list_and_none(self):
        """Test count method with empty list."""
        associated = TestAssociatedObjects([])
        self.assertEqual(0, associated.count())

        associated = TestAssociatedObjects(None)
        self.assertEqual(0, associated.count())


class TestPolicyConstants(unittest.TestCase):
    """Test cases for Policy constants."""

    def test_built_in_type_prefix(self):
        """Test BUILT_IN_TYPE_PREFIX constant."""
        self.assertEqual("system_", Policy.BUILT_IN_TYPE_PREFIX)
