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
from unittest.mock import patch

from gravitino import GravitinoClient
from gravitino.api.tag import Tag
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.api.tag.tag_change import TagChange
from gravitino.client.generic_tag import GenericTag
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.name_identifier import NameIdentifier
from gravitino.dto.responses.tag_response import (
    TagListResponse,
    TagNamesListResponse,
    TagResponse,
)
from tests.unittests import mock_base


@mock_base.mock_data
class TestTagAPI(unittest.TestCase):
    _metalake_name: str = "metalake_demo"

    def test_client_get_tag(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self._check_default_tag_a(retrieved_tag)

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

    def test_client_alter_tag_with_name(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self._check_default_tag_a(retrieved_tag)

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

    def test_client_alter_tag_with_comment(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self._check_default_tag_a(retrieved_tag)

            change = TagChange.update_comment("new comment")
            client.alter_tag("tagA", change)

            retrieved_tag = client.get_tag("tagA")
            self.assertEqual("tagA", retrieved_tag.name())
            self.assertEqual("new comment", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value1",
                    "key2": "value2",
                },
                retrieved_tag.properties(),
            )

    def test_client_alter_tag_with_remove_property(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self._check_default_tag_a(retrieved_tag)

            change = TagChange.remove_property("key1")
            client.alter_tag("tagA", change)

            retrieved_tag = client.get_tag("tagA")
            self.assertEqual("tagA", retrieved_tag.name())
            self.assertEqual("mock tag A", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key2": "value2",
                },
                retrieved_tag.properties(),
            )

    def test_client_alter_tag_with_add_property(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self._check_default_tag_a(retrieved_tag)

            change = TagChange.set_property("key3", "value3")
            client.alter_tag("tagA", change)

            retrieved_tag = client.get_tag("tagA")
            self.assertEqual("tagA", retrieved_tag.name())
            self.assertEqual("mock tag A", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value1",
                    "key2": "value2",
                    "key3": "value3",
                },
                retrieved_tag.properties(),
            )

    def test_client_alter_tag_with_replace_property(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagA")
            self._check_default_tag_a(retrieved_tag)

            change = TagChange.set_property("key1", "value3")
            client.alter_tag("tagA", change)

            retrieved_tag = client.get_tag("tagA")
            self.assertEqual("tagA", retrieved_tag.name())
            self.assertEqual("mock tag A", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value3",
                    "key2": "value2",
                },
                retrieved_tag.properties(),
            )

    def test_client_alter_tag_with_all_operations(self, *mock_method) -> None:
        with mock_base.mock_tag_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            retrieved_tag = client.get_tag("tagB")
            self._check_default_tag_b(retrieved_tag)

            changes = [
                TagChange.set_property("key1", "value3"),
                TagChange.remove_property("key2"),
                TagChange.update_comment("mock tag B updated"),
                TagChange.rename("new_tag_B"),
            ]

            client.alter_tag("tagB", *changes)
            retrieved_tag = client.get_tag("new_tag_B")

            self.assertEqual("new_tag_B", retrieved_tag.name())
            self.assertEqual("mock tag B updated", retrieved_tag.comment())
            self.assertEqual(
                {
                    "key1": "value3",
                },
                retrieved_tag.properties(),
            )

    def test_gravitino_list_tag_api(self, *mock_method) -> None:
        resp = TagNamesListResponse(0, ["tagA", "tagB", "tagC"])
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            tags = client.list_tags()
            self.assertEqual(3, len(tags))
            self.assertTrue("tagA" in tags)
            self.assertTrue("tagB" in tags)
            self.assertTrue("tagC" in tags)

    def test_gravitino_list_tag_info_api(self, *mock_method) -> None:
        tag = mock_base.build_tag_dto()
        resp = TagListResponse(0, [tag])
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            tags = client.list_tags_info()
            self.assertEqual(1, len(tags))
            self.check_tag_equal(tag, tags[0])

    def test_gravitino_metalake_delete_tag_api(self, *mock_method) -> None:
        resp = DropResponse(0, True)
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ):
            is_dropped = client.delete_tag("tag1")
            self.assertTrue(is_dropped)

    def test_gravitino_metalake_delete_tag_api_with_empty_tag_name(
        self, *mock_method
    ) -> None:
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with self.assertRaises(ValueError):
            client.delete_tag(" ")

    def test_gravitino_metalake_create_tag_api(self, *mock_method) -> None:
        tag = mock_base.build_tag_dto()
        resp = TagResponse(0, tag)
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ):
            created_tag = client.create_tag(tag.name(), tag.comment(), tag.properties())
            self.check_tag_equal(tag, created_tag)

    def test_gravitino_metalake_get_tag_api(self, *mock_method) -> None:
        tag = mock_base.build_tag_dto()
        resp = TagResponse(0, tag)
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            retrieved_tag = client.get_tag(tag.name())
            self.check_tag_equal(tag, retrieved_tag)

    def test_gravitino_metalake_get_tag_api_with_empty_tag_name(
        self, *mock_method
    ) -> None:
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with self.assertRaises(ValueError):
            client.get_tag(" ")

    def test_gravitino_metalake_alter_tag_api(self, *mock_method) -> None:
        tag = mock_base.build_tag_dto(
            "tagB",
            "mock tag B",
            {
                "key2": "value2",
            },
        )
        resp = TagResponse(0, tag)
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            rename_change = TagChange.rename("tagB")
            update_comment_change = TagChange.update_comment("mock tag B")
            update_properties_change = TagChange.set_property("key2", "value2")

            updated_tag = client.alter_tag(
                tag.name(),
                rename_change,
                update_comment_change,
                update_properties_change,
            )

            self.assertEqual("tagB", updated_tag.name())
            self.assertEqual("mock tag B", updated_tag.comment())
            self.assertEqual(
                {
                    "key2": "value2",
                },
                updated_tag.properties(),
            )
            mock_put.assert_called_once()

    def test_gravitino_metalake_alter_tag_api_with_empty_tag_name(
        self, *mock_args
    ) -> None:
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )

        with self.assertRaises(ValueError):
            client.alter_tag(" ", TagChange.rename("tagB"))

    def _check_default_tag_a(self, tag: Tag) -> None:
        self.assertEqual("tagA", tag.name())
        self.assertEqual("mock tag A", tag.comment())
        self.assertEqual(
            {
                "key1": "value1",
                "key2": "value2",
            },
            tag.properties(),
        )

    def _check_default_tag_b(self, tag: Tag) -> None:
        self.assertEqual("tagB", tag.name())
        self.assertEqual("mock tag B", tag.comment())
        self.assertEqual(
            {
                "key1": "value1",
                "key2": "value2",
            },
            tag.properties(),
        )

    def check_tag_equal(self, left: Tag, right: Tag) -> None:
        self.assertEqual(left.name(), right.name())
        self.assertEqual(left.comment(), right.comment())
        self.assertEqual(left.properties(), right.properties())


@mock_base.mock_data
class TestObjectTagAPI(unittest.TestCase):
    """Tests for object-level tag operations on GenericFileset."""  # pylint: disable=protected-access

    _metalake_name: str = "metalake_demo"

    def _load_fileset(self, client):
        """Helper to load a test fileset."""
        catalog = client.load_catalog("fileset_catalog")
        return catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of("tmp", "fileset")
        )

    def test_fileset_supports_tags(self, *mock_method) -> None:
        """Test that GenericFileset is an instance of SupportsTags."""
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )
        fileset = self._load_fileset(client)
        self.assertIsInstance(fileset, SupportsTags)

    def test_fileset_list_tags(self, *mock_method) -> None:
        """Test listing tags on a fileset via REST API."""
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )
        fileset = self._load_fileset(client)

        with patch.object(
            fileset._object_tag_operations,
            "list_tags",
            return_value=["tagA", "tagB"],
        ):
            tags = fileset.list_tags()
            self.assertEqual(2, len(tags))
            self.assertIn("tagA", tags)
            self.assertIn("tagB", tags)

    def test_fileset_list_tags_info(self, *mock_method) -> None:
        """Test listing tags with details on a fileset via REST API."""
        tag_dto = mock_base.build_tag_dto("tagA", "comment A")
        mock_tag = GenericTag("metalake_demo", tag_dto, None)

        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )
        fileset = self._load_fileset(client)

        with patch.object(
            fileset._object_tag_operations,
            "list_tags_info",
            return_value=[mock_tag],
        ):
            tags = fileset.list_tags_info()
            self.assertEqual(1, len(tags))
            self.assertEqual("tagA", tags[0].name())
            self.assertEqual("comment A", tags[0].comment())

    def test_fileset_get_tag(self, *mock_method) -> None:
        """Test getting a specific tag on a fileset via REST API."""
        tag_dto = mock_base.build_tag_dto("tagA", "comment A")
        mock_tag = GenericTag("metalake_demo", tag_dto, None)

        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )
        fileset = self._load_fileset(client)

        with patch.object(
            fileset._object_tag_operations,
            "get_tag",
            return_value=mock_tag,
        ):
            tag = fileset.get_tag("tagA")
            self.assertEqual("tagA", tag.name())
            self.assertEqual("comment A", tag.comment())

    def test_fileset_get_tag_with_empty_name(self, *mock_method) -> None:
        """Test that get_tag raises ValueError for empty tag name."""
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )
        fileset = self._load_fileset(client)

        with self.assertRaises(IllegalArgumentException):
            fileset.get_tag(" ")

    def test_fileset_associate_tags(self, *mock_method) -> None:
        """Test associating and removing tags on a fileset via REST API."""
        client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self._metalake_name,
            check_version=False,
        )
        fileset = self._load_fileset(client)

        with patch.object(
            fileset._object_tag_operations,
            "associate_tags",
            return_value=["tagA", "tagB"],
        ) as mock_assoc:
            result = fileset.associate_tags(["tagA", "tagB"], [])
            self.assertEqual(2, len(result))
            self.assertIn("tagA", result)
            self.assertIn("tagB", result)
            mock_assoc.assert_called_once_with(["tagA", "tagB"], [])
