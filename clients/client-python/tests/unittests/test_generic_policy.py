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

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.client.generic_policy import GenericPolicy
from gravitino.client.generic_policy_tag_association import (
    GenericPolicyTagAssociation,
)
from gravitino.api.policy.policy_association_selector import (
    AllValuesSelector,
    TagValueSelector,
)
from gravitino.dto.tag_dto import TagDTO
from tests.unittests import mock_base


class TestGenericPolicy(unittest.TestCase):
    def test_delegation(self) -> None:
        policy_dto = mock_base.build_policy_dto()
        policy = GenericPolicy(policy_dto)

        self.assertEqual("policyA", policy.name())
        self.assertEqual("system_access_control", policy.policy_type())
        self.assertEqual("commentA", policy.comment())
        self.assertTrue(policy.enabled())
        self.assertEqual(False, policy.inherited())
        self.assertEqual(policy_dto.audit_info(), policy.audit_info())

    def test_content_converted_to_domain(self) -> None:
        policy = GenericPolicy(mock_base.build_policy_dto())
        content = policy.content()
        self.assertEqual(
            PolicyContents.access_control([Privilege.Name.SELECT_TABLE], ["analyst"]),
            content,
        )

    def test_equality_and_hash(self) -> None:
        left = GenericPolicy(mock_base.build_policy_dto())
        right = GenericPolicy(mock_base.build_policy_dto())
        self.assertEqual(left, right)
        self.assertEqual(hash(left), hash(right))
        self.assertNotEqual(left, GenericPolicy(mock_base.build_policy_dto("policyB")))
        self.assertNotEqual(left, mock_base.build_policy_dto())


class TestGenericPolicyTagAssociation(unittest.TestCase):
    def test_accessors_and_equality(self) -> None:
        policy = GenericPolicy(mock_base.build_policy_dto())
        tag = TagDTO.builder().name("tagA").build()
        selector = TagValueSelector.of("gold")

        association = GenericPolicyTagAssociation(policy, tag, selector)
        self.assertEqual(policy, association.policy())
        self.assertEqual(tag, association.tag())
        self.assertEqual(selector, association.selector())

        same = GenericPolicyTagAssociation(
            GenericPolicy(mock_base.build_policy_dto()),
            TagDTO.builder().name("tagA").build(),
            TagValueSelector.of("gold"),
        )
        self.assertEqual(association, same)
        self.assertEqual(hash(association), hash(same))

        different = GenericPolicyTagAssociation(policy, tag, AllValuesSelector.get())
        self.assertNotEqual(association, different)


if __name__ == "__main__":
    unittest.main()
