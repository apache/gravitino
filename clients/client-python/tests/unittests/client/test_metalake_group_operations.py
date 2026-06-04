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
from unittest.mock import patch

from gravitino.client.gravitino_client import GravitinoClient
from gravitino.dto.authorization.group_dto import GroupDTO
from gravitino.dto.requests.group_add_request import GroupAddRequest
from gravitino.dto.responses.remove_response import RemoveResponse
from gravitino.dto.responses.group_response import (
    GroupListResponse,
    GroupNamesListResponse,
    GroupResponse,
)
from gravitino.exceptions.base import (
    IllegalArgumentException,
    NoSuchMetalakeException,
    NoSuchGroupException,
    GroupAlreadyExistsException,
)
from gravitino.exceptions.handlers.group_error_handler import GROUP_ERROR_HANDLER
from tests.unittests import mock_base


def _build_group_dto(name: str = "engineers", roles: list | None = None) -> GroupDTO:
    return (
        GroupDTO.builder()
        .with_name(name)
        .with_roles(roles if roles is not None else [])
        .with_audit(mock_base.build_audit_info())
        .build()
    )


class TestMetalakeGroupOperations(unittest.TestCase):
    METALAKE_GROUPS_PATH = "api/metalakes/metalake_demo/groups"
    METALAKE_GROUP_PATH = "api/metalakes/metalake_demo/groups/engineers"

    def test_add_group(self):
        metalake = mock_base.mock_load_metalake()
        group = _build_group_dto()
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ) as mock_post:
            added = metalake.add_group("engineers")

            self.assertEqual("engineers", added.name())
            self.assertEqual([], added.roles())

            mock_post.assert_called_once()
            call_args = mock_post.call_args
            self.assertEqual(self.METALAKE_GROUPS_PATH, call_args.args[0])
            self.assertIsInstance(call_args.kwargs["json"], GroupAddRequest)
            self.assertIn('"name": "engineers"', call_args.kwargs["json"].to_json())
            self.assertIs(GROUP_ERROR_HANDLER, call_args.kwargs["error_handler"])

    def test_add_group_already_exists(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=GroupAlreadyExistsException("group engineers already exists"),
        ):
            with self.assertRaises(GroupAlreadyExistsException):
                metalake.add_group("engineers")

    def test_add_group_empty_name_rejected(self):
        metalake = mock_base.mock_load_metalake()
        with self.assertRaises(IllegalArgumentException):
            metalake.add_group("")

    def test_get_group(self):
        metalake = mock_base.mock_load_metalake()
        group = _build_group_dto(roles=["role_a", "role_b"])
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            retrieved = metalake.get_group("engineers")

            self.assertEqual("engineers", retrieved.name())
            self.assertEqual(["role_a", "role_b"], retrieved.roles())

            mock_get.assert_called_once()
            self.assertEqual(self.METALAKE_GROUP_PATH, mock_get.call_args.args[0])
            self.assertIs(
                GROUP_ERROR_HANDLER, mock_get.call_args.kwargs["error_handler"]
            )

    def test_get_group_not_found(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchGroupException("no such group"),
        ):
            with self.assertRaises(NoSuchGroupException):
                metalake.get_group("engineers")

    def test_remove_group(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(RemoveResponse(0, True).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ) as mock_delete:
            self.assertTrue(metalake.remove_group("engineers"))

            mock_delete.assert_called_once()
            self.assertEqual(self.METALAKE_GROUP_PATH, mock_delete.call_args.args[0])
            self.assertIs(
                GROUP_ERROR_HANDLER, mock_delete.call_args.kwargs["error_handler"]
            )

    def test_remove_group_returns_false_when_not_removed(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(RemoveResponse(0, False).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            self.assertFalse(metalake.remove_group("engineers"))

    def test_list_groups(self):
        metalake = mock_base.mock_load_metalake()
        groups = [_build_group_dto("alice"), _build_group_dto("bob")]
        mock_resp = mock_base.mock_http_response(GroupListResponse(0, groups).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            result = metalake.list_groups()

            self.assertEqual(["alice", "bob"], [u.name() for u in result])

            mock_get.assert_called_once_with(
                self.METALAKE_GROUPS_PATH,
                params={"details": "true"},
                error_handler=GROUP_ERROR_HANDLER,
            )

    def test_list_group_names(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(
            GroupNamesListResponse(0, ["alice", "bob"]).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            names = metalake.list_group_names()

            self.assertEqual(["alice", "bob"], names)

            mock_get.assert_called_once_with(
                self.METALAKE_GROUPS_PATH, error_handler=GROUP_ERROR_HANDLER
            )

    def test_list_groups_metalake_not_found(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchMetalakeException("metalake not found"),
        ):
            with self.assertRaises(NoSuchMetalakeException):
                metalake.list_groups()


class TestGravitinoClientGroupDelegates(unittest.TestCase):
    """Verify that GravitinoClient correctly delegates Group operations."""

    def _make_client(self):
        client = GravitinoClient.__new__(GravitinoClient)
        return client

    def test_client_add_group(self):
        client = self._make_client()
        group = _build_group_dto()
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
            ),
        ):
            result = client.add_group("engineers")
            self.assertEqual("engineers", result.name())

    def test_client_get_group(self):
        client = self._make_client()
        group = _build_group_dto(roles=["r1"])
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch("gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp),
        ):
            result = client.get_group("engineers")
            self.assertEqual(["r1"], result.roles())

    def test_client_remove_group(self):
        client = self._make_client()
        mock_resp = mock_base.mock_http_response(RemoveResponse(0, True).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
            ),
        ):
            self.assertTrue(client.remove_group("engineers"))

    def test_client_list_groups(self):
        client = self._make_client()
        groups = [_build_group_dto("alice"), _build_group_dto("bob")]
        mock_resp = mock_base.mock_http_response(GroupListResponse(0, groups).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch("gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp),
        ):
            result = client.list_groups()
            self.assertEqual(["alice", "bob"], [u.name() for u in result])

    def test_client_list_group_names(self):
        client = self._make_client()
        mock_resp = mock_base.mock_http_response(
            GroupNamesListResponse(0, ["alice", "bob"]).to_json()
        )
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch("gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp),
        ):
            result = client.list_group_names()
            self.assertEqual(["alice", "bob"], result)
