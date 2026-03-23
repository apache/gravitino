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

from gravitino.api.policy import PolicyChange, RenamePolicy, UpdatePolicyComment


class TestPolicyChange(unittest.TestCase):
    def test_rename_policy(self) -> None:
        change1, change2 = (
            PolicyChange.rename(f"new_policy_name{i + 1}") for i in range(2)
        )
        change3 = PolicyChange.rename("new_policy_name1")

        self.assertTrue(isinstance(change1, RenamePolicy))

        self.assertNotEqual(change1, change2)
        self.assertNotEqual(change2, change3)
        self.assertEqual(change1, change3)

        self.assertNotEqual(hash(change1), hash(change2))
        self.assertNotEqual(hash(change2), hash(change3))
        self.assertEqual(hash(change1), hash(change3))

        self.assertEqual("new_policy_name1", change1.new_name())
        self.assertEqual("RENAME POLICY new_policy_name1", str(change1))

    def test_update_comment_for_policy(self) -> None:
        change1, change2 = PolicyChange.update_comment(
            f"new_comment{i + 1}" for i in range(2)
        )
        change3 = PolicyChange.update_comment("new_comment1")

        self.assertTrue(isinstance(change1, UpdatePolicyComment))

        self.assertNotEqual(change1, change2)
        self.assertNotEqual(change2, change3)
        self.assertEqual(change1, change3)

        self.assertNotEqual(hash(change1), hash(change2))
        self.assertNotEqual(hash(change2), hash(change3))
        self.assertEqual(hash(change1), hash(change3))

        self.assertEqual("new_comment1", change1.new_comment())
        self.assertEqual("UPDATE POLICY COMMENT new_comment1", str(change1))

    def test_update_content_for_policy(self) -> None:
        new_content1 = {"key": "value1"}
        new_content2 = {"key": "value2"}
        change1, change2 = (
            PolicyChange.update_content("policy_type", new_content1),
            PolicyChange.update_content("policy_type", new_content2),
        )
        change3 = PolicyChange.update_content("policy_type", new_content1)

        self.assertTrue(isinstance(change1, PolicyChange))

        self.assertNotEqual(change1, change2)
        self.assertNotEqual(change2, change3)
        self.assertEqual(change1, change3)

        self.assertNotEqual(hash(change1), hash(change2))
        self.assertNotEqual(hash(change2), hash(change3))
        self.assertEqual(hash(change1), hash(change3))

        self.assertEqual("policy_type", change1.policy_type())
        self.assertEqual(new_content1, change1.new_content())
        self.assertEqual(
            "UPDATE POLICY CONTENT policy_type with content {'key': 'value1'}",
            str(change1),
        )
