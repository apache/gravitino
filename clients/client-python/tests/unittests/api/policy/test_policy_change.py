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

from gravitino.api.policy.policy_change import PolicyChange
from gravitino.api.policy.policy_contents import PolicyContents


class TestPolicyChange(unittest.TestCase):
    def test_rename(self) -> None:
        expected_new_name = "new_name"
        policy_change = PolicyChange.rename(expected_new_name)
        self.assertEqual(expected_new_name, policy_change.new_name)
        self.assertEqual(f"RENAME POLICY {expected_new_name}", str(policy_change))

    def test_update_comment(self) -> None:
        expected_new_comment = "new_comment"
        policy_change = PolicyChange.update_comment(expected_new_comment)
        self.assertEqual(expected_new_comment, policy_change.new_comment)
        self.assertEqual(
            f"UPDATE POLICY COMMENT {expected_new_comment}", str(policy_change)
        )

    def test_update_content(self) -> None:
        content = PolicyContents.row_filter("age > 18")
        policy_change = PolicyChange.update_content("system_row_filter", content)
        self.assertEqual("system_row_filter", policy_change.policy_type)
        self.assertEqual(content, policy_change.content)
        self.assertEqual(
            f"UPDATE POLICY CONTENT policyType=system_row_filter, content={content}",
            str(policy_change),
        )

    def test_update_content_hash_with_nested_custom_rules(self) -> None:
        content = PolicyContents.custom({"rule": ["a", "b"]})
        policy_change = PolicyChange.update_content("custom", content)
        equal_change = PolicyChange.update_content(
            "custom", PolicyContents.custom({"rule": ["a", "b"]})
        )
        self.assertEqual(policy_change, equal_change)
        self.assertEqual(hash(policy_change), hash(equal_change))

    def test_equality(self) -> None:
        self.assertEqual(PolicyChange.rename("a"), PolicyChange.rename("a"))
        self.assertNotEqual(PolicyChange.rename("a"), PolicyChange.rename("b"))
        self.assertNotEqual(PolicyChange.rename("a"), PolicyChange.update_comment("a"))
