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
from gravitino.dto.authorization.group_dto import GroupDTO
from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.authorization.user_dto import UserDTO
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.group_response import (
    GroupNamesListResponse,
    GroupResponse,
)
from gravitino.dto.responses.remove_response import RemoveResponse
from gravitino.dto.responses.role_response import RoleNamesListResponse, RoleResponse
from gravitino.dto.responses.user_response import (
    UserListResponse,
    UserNamesListResponse,
    UserResponse,
)
from tests.unittests import mock_base


@mock_base.mock_data
class TestAuthorizationAPI(unittest.TestCase):
    _metalake_name: str = "metalake_demo"

    # ========== User ==========

    def test_client_add_user(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            user = client.add_user("new_user")
            self.assertEqual("new_user", user.name())

    def test_client_get_user(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            user = client.get_user("userA")
            self.assertEqual("userA", user.name())

    def test_client_list_user_names(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            names = client.list_user_names()
            self.assertIn("userA", names)
            self.assertIn("userB", names)

    def test_client_list_users(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            users = client.list_users()
            user_names = [u.name() for u in users]
            self.assertIn("userA", user_names)
            self.assertIn("userB", user_names)

    def test_client_remove_user(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            result = client.remove_user("userA")
            self.assertTrue(result)

            with self.assertRaises(ValueError):
                client.get_user("userA")

    # ========== Group ==========

    def test_client_add_group(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            group = client.add_group("new_group")
            self.assertEqual("new_group", group.name())

    def test_client_get_group(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            group = client.get_group("groupA")
            self.assertEqual("groupA", group.name())

    def test_client_list_group_names(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            names = client.list_group_names()
            self.assertIn("groupA", names)

    def test_client_remove_group(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            result = client.remove_group("groupA")
            self.assertTrue(result)

    # ========== Role ==========

    def test_client_create_role(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            role = client.create_role("new_role", {"purpose": "test"}, [])
            self.assertEqual("new_role", role.name())
            self.assertEqual({"purpose": "test"}, role.properties())

    def test_client_get_role(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            role = client.get_role("roleA")
            self.assertEqual("roleA", role.name())

    def test_client_list_role_names(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            names = client.list_role_names()
            self.assertIn("roleA", names)

    def test_client_delete_role(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            result = client.delete_role("roleA")
            self.assertTrue(result)

    # ========== Grant / Revoke Roles ==========

    def test_client_grant_revoke_roles_to_user(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            user = client.grant_roles_to_user(["roleA"], "userA")
            self.assertIn("roleA", user.roles())

            user = client.revoke_roles_from_user(["roleA"], "userA")
            self.assertNotIn("roleA", user.roles())

    def test_client_grant_revoke_roles_to_group(self, *mock_method) -> None:
        with mock_base.mock_authorization_methods():
            client = GravitinoClient(
                uri="http://localhost:8090",
                metalake_name=self._metalake_name,
                check_version=False,
            )

            group = client.grant_roles_to_group(["roleA"], "groupA")
            self.assertIn("roleA", group.roles())

            group = client.revoke_roles_from_group(["roleA"], "groupA")
            self.assertNotIn("roleA", group.roles())

    # ========== HTTP mock tests ==========

    def test_gravitino_add_user_api(self, *mock_method) -> None:
        user_dto = UserDTO.builder().with_name("api_user").with_roles([]).build()
        resp = UserResponse(0, user_dto)
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
            user = client.add_user("api_user")
            self.assertEqual("api_user", user.name())

    def test_gravitino_get_user_api(self, *mock_method) -> None:
        user_dto = UserDTO.builder().with_name("api_user").with_roles(["role1"]).build()
        resp = UserResponse(0, user_dto)
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
            user = client.get_user("api_user")
            self.assertEqual("api_user", user.name())
            self.assertIn("role1", user.roles())

    def test_gravitino_list_user_names_api(self, *mock_method) -> None:
        resp = UserNamesListResponse(0, ["user1", "user2"])
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
            names = client.list_user_names()
            self.assertEqual(2, len(names))
            self.assertIn("user1", names)

    def test_gravitino_list_users_api(self, *mock_method) -> None:
        user1 = UserDTO.builder().with_name("user1").build()
        user2 = UserDTO.builder().with_name("user2").build()
        resp = UserListResponse(0, [user1, user2])
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
            users = client.list_users()
            self.assertEqual(2, len(users))
            self.assertEqual("user1", users[0].name())

    def test_gravitino_remove_user_api(self, *mock_method) -> None:
        resp = RemoveResponse(0, True)
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
            result = client.remove_user("user1")
            self.assertTrue(result)

    def test_gravitino_add_group_api(self, *mock_method) -> None:
        group_dto = GroupDTO.builder().with_name("api_group").build()
        resp = GroupResponse(0, group_dto)
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
            group = client.add_group("api_group")
            self.assertEqual("api_group", group.name())

    def test_gravitino_list_group_names_api(self, *mock_method) -> None:
        resp = GroupNamesListResponse(0, ["group1", "group2"])
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
            names = client.list_group_names()
            self.assertEqual(2, len(names))

    def test_gravitino_remove_group_api(self, *mock_method) -> None:
        resp = RemoveResponse(0, True)
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
            result = client.remove_group("group1")
            self.assertTrue(result)

    def test_gravitino_create_role_api(self, *mock_method) -> None:
        role_dto = (
            RoleDTO.builder()
            .with_name("api_role")
            .with_properties({"purpose": "test"})
            .build()
        )
        resp = RoleResponse(0, role_dto)
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
            role = client.create_role("api_role", {"purpose": "test"}, [])
            self.assertEqual("api_role", role.name())

    def test_gravitino_get_role_api(self, *mock_method) -> None:
        role_dto = (
            RoleDTO.builder().with_name("api_role").with_properties({"k": "v"}).build()
        )
        resp = RoleResponse(0, role_dto)
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
            role = client.get_role("api_role")
            self.assertEqual("api_role", role.name())
            self.assertEqual({"k": "v"}, role.properties())

    def test_gravitino_list_role_names_api(self, *mock_method) -> None:
        resp = RoleNamesListResponse(0, ["role1", "role2"])
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
            names = client.list_role_names()
            self.assertEqual(2, len(names))
            self.assertIn("role1", names)

    def test_gravitino_delete_role_api(self, *mock_method) -> None:
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
            result = client.delete_role("role1")
            self.assertTrue(result)

    def test_gravitino_grant_roles_to_user_api(self, *mock_method) -> None:
        user_dto = UserDTO.builder().with_name("user1").with_roles(["role1"]).build()
        resp = UserResponse(0, user_dto)
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
            user = client.grant_roles_to_user(["role1"], "user1")
            self.assertEqual("user1", user.name())
            self.assertIn("role1", user.roles())
            mock_put.assert_called_once()

    def test_gravitino_revoke_roles_from_user_api(self, *mock_method) -> None:
        user_dto = UserDTO.builder().with_name("user1").with_roles([]).build()
        resp = UserResponse(0, user_dto)
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
        ):
            user = client.revoke_roles_from_user(["role1"], "user1")
            self.assertNotIn("role1", user.roles())

    def test_gravitino_grant_roles_to_group_api(self, *mock_method) -> None:
        group_dto = GroupDTO.builder().with_name("group1").with_roles(["role1"]).build()
        resp = GroupResponse(0, group_dto)
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
        ):
            group = client.grant_roles_to_group(["role1"], "group1")
            self.assertIn("role1", group.roles())

    def test_gravitino_revoke_roles_from_group_api(self, *mock_method) -> None:
        group_dto = GroupDTO.builder().with_name("group1").with_roles([]).build()
        resp = GroupResponse(0, group_dto)
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
        ):
            group = client.revoke_roles_from_group(["role1"], "group1")
            self.assertNotIn("role1", group.roles())
