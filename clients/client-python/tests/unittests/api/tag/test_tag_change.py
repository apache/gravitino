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

from gravitino.api.tag.tag_change import TagChange


class TestTagChange(unittest.TestCase):
    def test_rename(self) -> None:
        expected_new_name = "new_name"
        tag_change = TagChange.rename(expected_new_name)
        self.assertEqual(expected_new_name, tag_change.new_name)
        self.assertEqual(f"RENAMETAG {expected_new_name}", str(tag_change))

    def test_update_comment(self) -> None:
        expected_new_comment = "new_comment"
        tag_change = TagChange.update_comment(expected_new_comment)
        self.assertEqual(expected_new_comment, tag_change.new_comment)
        self.assertEqual(f"UPDATETAGCOMMENT {expected_new_comment}", str(tag_change))

    def test_set_property(self) -> None:
        expected_property = "property"
        expected_value = "value"
        tag_change = TagChange.set_property(expected_property, expected_value)
        self.assertEqual(expected_property, tag_change.name)
        self.assertEqual(expected_value, tag_change.value)
        self.assertEqual(
            f"SETTAGPROPERTY {expected_property} = {expected_value}", str(tag_change)
        )

    def test_remove_property(self) -> None:
        expected_property = "property"
        tag_change = TagChange.remove_property(expected_property)
        self.assertEqual(expected_property, tag_change.removed_property)
        self.assertEqual(f"REMOVETAGPROPERTY {expected_property}", str(tag_change))
