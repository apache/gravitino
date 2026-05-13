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
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.authorization.user_dto import UserDTO
from gravitino.dto.requests.user_add_request import UserAddRequest
from gravitino.dto.responses.remove_response import RemoveResponse
from gravitino.dto.responses.user_response import (
    UserListResponse,
    UserNamesListResponse,
    UserResponse,
)
from gravitino.exceptions.base import (
    IllegalArgumentException,
    NoSuchMetalakeException,
    NoSuchUserException,
    UserAlreadyExistsException,
)
from gravitino.exceptions.handlers.user_error_handler import USER_ERROR_HANDLER
from tests.unittests import mock_base


def _build_user_dto(name: str = "alice", roles: list | None = None) -> UserDTO:
    return (
        UserDTO.builder()
        .with_name(name)
        .with_roles(roles if roles is not None else [])
        .with_audit(mock_base.build_audit_info())
        .build()
    )


class TestMetalakeUserOperations(unittest.TestCase):
    METALAKE_USERS_PATH = "api/metalakes/metalake_demo/users"
    METALAKE_USER_PATH = "api/metalakes/metalake_demo/users/alice"

    def test_add_user(self):
        metalake = mock_base.mock_load_metalake()
        user = _build_user_dto()
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ) as mock_post:
            added = metalake.add_user("alice")

            self.assertEqual("alice", added.name())
            self.assertEqual([], added.roles())

            mock_post.assert_called_once()
            call_args = mock_post.call_args
            self.assertEqual(self.METALAKE_USERS_PATH, call_args.args[0])
            self.assertIsInstance(call_args.kwargs["json"], UserAddRequest)
            self.assertIn('"name": "alice"', call_args.kwargs["json"].to_json())
            self.assertIs(USER_ERROR_HANDLER, call_args.kwargs["error_handler"])

    def test_add_user_already_exists(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=UserAlreadyExistsException("user alice already exists"),
        ):
            with self.assertRaises(UserAlreadyExistsException):
                metalake.add_user("alice")

    def test_add_user_empty_name_rejected(self):
        metalake = mock_base.mock_load_metalake()
        with self.assertRaises(IllegalArgumentException):
            metalake.add_user("")

    def test_get_user(self):
        metalake = mock_base.mock_load_metalake()
        user = _build_user_dto(roles=["role_a", "role_b"])
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            retrieved = metalake.get_user("alice")

            self.assertEqual("alice", retrieved.name())
            self.assertEqual(["role_a", "role_b"], retrieved.roles())

            mock_get.assert_called_once()
            self.assertEqual(self.METALAKE_USER_PATH, mock_get.call_args.args[0])
            self.assertIs(
                USER_ERROR_HANDLER, mock_get.call_args.kwargs["error_handler"]
            )

    def test_get_user_not_found(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchUserException("no such user"),
        ):
            with self.assertRaises(NoSuchUserException):
                metalake.get_user("alice")

    def test_remove_user(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(RemoveResponse(0, True).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ) as mock_delete:
            self.assertTrue(metalake.remove_user("alice"))

            mock_delete.assert_called_once()
            self.assertEqual(self.METALAKE_USER_PATH, mock_delete.call_args.args[0])
            self.assertIs(
                USER_ERROR_HANDLER, mock_delete.call_args.kwargs["error_handler"]
            )

    def test_remove_user_returns_false_when_not_removed(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(RemoveResponse(0, False).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            self.assertFalse(metalake.remove_user("alice"))

    def test_list_users(self):
        metalake = mock_base.mock_load_metalake()
        users = [_build_user_dto("alice"), _build_user_dto("bob")]
        mock_resp = mock_base.mock_http_response(UserListResponse(0, users).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            result = metalake.list_users()

            self.assertEqual(["alice", "bob"], [u.name() for u in result])

            mock_get.assert_called_once_with(
                self.METALAKE_USERS_PATH,
                params={"details": "true"},
                error_handler=USER_ERROR_HANDLER,
            )

    def test_list_user_names(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(
            UserNamesListResponse(0, ["alice", "bob"]).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ) as mock_get:
            names = metalake.list_user_names()

            self.assertEqual(["alice", "bob"], names)

            mock_get.assert_called_once_with(
                self.METALAKE_USERS_PATH, error_handler=USER_ERROR_HANDLER
            )

    def test_list_users_metalake_not_found(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchMetalakeException("metalake not found"),
        ):
            with self.assertRaises(NoSuchMetalakeException):
                metalake.list_users()


class TestGravitinoClientUserDelegates(unittest.TestCase):
    """Verify that GravitinoClient correctly delegates User operations."""

    def _make_client(self):
        metalake = mock_base.mock_load_metalake()
        client = GravitinoClient.__new__(GravitinoClient)
        client._metalake = metalake
        return client, metalake

    def test_client_add_user(self):
        client, metalake = self._make_client()
        user = _build_user_dto()
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())
        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ):
            result = client.add_user("alice")
            self.assertEqual("alice", result.name())

    def test_client_get_user(self):
        client, metalake = self._make_client()
        user = _build_user_dto(roles=["r1"])
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())
        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result = client.get_user("alice")
            self.assertEqual(["r1"], result.roles())

    def test_client_remove_user(self):
        client, metalake = self._make_client()
        mock_resp = mock_base.mock_http_response(RemoveResponse(0, True).to_json())
        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            self.assertTrue(client.remove_user("alice"))

    def test_client_list_users(self):
        client, metalake = self._make_client()
        users = [_build_user_dto("alice"), _build_user_dto("bob")]
        mock_resp = mock_base.mock_http_response(UserListResponse(0, users).to_json())
        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result = client.list_users()
            self.assertEqual(["alice", "bob"], [u.name() for u in result])

    def test_client_list_user_names(self):
        client, metalake = self._make_client()
        mock_resp = mock_base.mock_http_response(
            UserNamesListResponse(0, ["alice", "bob"]).to_json()
        )
        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            result = client.list_user_names()
            self.assertEqual(["alice", "bob"], result)
