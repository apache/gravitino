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

from __future__ import annotations

import unittest

from gravitino import GravitinoClient
from gravitino.api.tag.tag_change import TagChange
from tests.unittests import mock_base


@mock_base.mock_data
class TestTagAPi(unittest.TestCase):
    _metalake_name: str = "metalake_demo"

    def test_client_get_api(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self.assertEqual("tagA", retrieved_tag.name())
            self.assertEqual("mock tag A", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value1",
                    "key2": "value2",
                },
                retrieved_tag.properties(),
            )

    def test_client_list_tag(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tags = client.list_tags()
            self.assertEqual(2, len(retrieved_tags))
            self.assertTrue("tagA" in retrieved_tags)
            self.assertTrue("tagB" in retrieved_tags)

            client.create_tag("tagC", "mock tag C", None)

            retrieved_tags = client.list_tags()
            self.assertEqual(3, len(retrieved_tags))
            self.assertTrue("tagA" in retrieved_tags)
            self.assertTrue("tagB" in retrieved_tags)
            self.assertTrue("tagC" in retrieved_tags)

    def test_client_list_tag_info(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tags = client.list_tags_info()
            self.assertEqual(2, len(retrieved_tags))
            tag_names = [tag.name() for tag in retrieved_tags]

            self.assertTrue("tagA" in tag_names)
            self.assertTrue("tagB" in tag_names)

            tag_comments = [tag.comment() for tag in retrieved_tags]
            self.assertTrue("mock tag A" in tag_comments)
            self.assertTrue("mock tag B" in tag_comments)

    def test_client_remove_tag(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tags = client.list_tags()
            self.assertEqual(2, len(retrieved_tags))
            self.assertTrue("tagA" in retrieved_tags)
            self.assertTrue("tagB" in retrieved_tags)

            client.delete_tag("tagA")
            retrieved_tags = client.list_tags()
            self.assertEqual(1, len(retrieved_tags))
            self.assertTrue("tagA" not in retrieved_tags)
            self.assertTrue("tagB" in retrieved_tags)

    def test_client_create_tag(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tags = client.list_tags()
            self.assertEqual(2, len(retrieved_tags))
            self.assertTrue("tagA" in retrieved_tags)
            self.assertTrue("tagB" in retrieved_tags)

            client.create_tag("tagC", "mock tag C", None)
            retrieved_tags = client.list_tags()
            self.assertEqual(3, len(retrieved_tags))
            self.assertTrue("tagA" in retrieved_tags)
            self.assertTrue("tagB" in retrieved_tags)
            self.assertTrue("tagC" in retrieved_tags)

    def test_client_rename_tag(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self.assertEqual("tagA", retrieved_tag.name())
            self.assertEqual("mock tag A", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value1",
                    "key2": "value2",
                },
                retrieved_tag.properties(),
            )

            change = TagChange.rename("tagA-new")
            client.alter_tag("tagA", change)

            with self.assertRaises(ValueError):
                client.get_tag("tagA")

            retrieved_tag = client.get_tag("tagA-new")
            self.assertEqual("tagA-new", retrieved_tag.name())
            self.assertEqual("mock tag A", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value1",
                    "key2": "value2",
                },
                retrieved_tag.properties(),
            )
