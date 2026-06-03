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

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.metadata_object import MetadataObject
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.group_dto import GroupDTO
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO
from gravitino.dto.authorization.user_dto import UserDTO
from gravitino.dto.requests.role_create_request import RoleCreateRequest
from gravitino.dto.requests.role_grant_request import RoleGrantRequest
from gravitino.dto.requests.role_revoke_request import RoleRevokeRequest
from gravitino.dto.requests.privilege_grant_request import PrivilegeGrantRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.group_response import GroupResponse
from gravitino.dto.responses.role_response import (
    RoleNamesListResponse,
    RoleResponse,
)
from gravitino.dto.responses.user_response import UserResponse
from gravitino.exceptions.base import (
    IllegalArgumentException,
    NoSuchRoleException,
    RoleAlreadyExistsException,
)
from gravitino.exceptions.handlers.permission_error_handler import (
    PERMISSION_ERROR_HANDLER,
)
from gravitino.exceptions.handlers.role_error_handler import ROLE_ERROR_HANDLER
from tests.unittests import mock_base


def _audit() -> AuditDTO:
    return AuditDTO(_creator="admin", _create_time="2024-01-01T00:00:00Z")


def _build_role_dto(
    name: str = "admin_role",
    props: dict | None = None,
    sec_objs: list | None = None,
) -> RoleDTO:
    return (
        RoleDTO.builder()
        .with_name(name)
        .with_properties(props)
        .with_securable_objects(sec_objs or [])
        .with_audit(_audit())
        .build()
    )


def _build_user_dto(name: str = "alice", roles: list | None = None) -> UserDTO:
    return (
        UserDTO.builder()
        .with_name(name)
        .with_roles(roles or [])
        .with_audit(_audit())
        .build()
    )


def _build_group_dto(name: str = "engineers", roles: list | None = None) -> GroupDTO:
    return (
        GroupDTO.builder()
        .with_name(name)
        .with_roles(roles if roles is not None else [])
        .with_audit(_audit())
        .build()
    )


class TestMetalakeRoleOperations(unittest.TestCase):
    METALAKE_ROLES_PATH = "api/metalakes/metalake_demo/roles"
    METALAKE_ROLE_PATH = "api/metalakes/metalake_demo/roles/admin_role"
    PERMISSIONS_USER_GRANT_PATH = (
        "api/metalakes/metalake_demo/permissions/users/alice/grant"
    )
    PERMISSIONS_USER_REVOKE_PATH = (
        "api/metalakes/metalake_demo/permissions/users/alice/revoke"
    )
    PERMISSIONS_GROUP_GRANT_PATH = (
        "api/metalakes/metalake_demo/permissions/groups/engineers/grant"
    )
    PERMISSIONS_GROUP_REVOKE_PATH = (
        "api/metalakes/metalake_demo/permissions/groups/engineers/revoke"
    )
    PERMISSIONS_ROLE_GRANT_PATH = "api/metalakes/metalake_demo/permissions/roles/admin_role/catalog/my_catalog/grant"
    PERMISSIONS_ROLE_REVOKE_PATH = "api/metalakes/metalake_demo/permissions/roles/admin_role/catalog/my_catalog/revoke"

    def test_create_role(self):
        metalake = mock_base.mock_load_metalake()
        sec_objs = [
            SecurableObjectDTO(
                "my_catalog",
                MetadataObject.Type.CATALOG,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
        ]
        role = _build_role_dto(sec_objs=sec_objs)
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ) as mock_post:
            result = metalake.create_role(
                "admin_role",
                properties=None,
                securable_objects=[
                    SecurableObjectDTO(
                        "my_catalog",
                        MetadataObject.Type.CATALOG,
                        [
                            PrivilegeDTO(
                                Privilege.Name.USE_CATALOG,
                                Privilege.Condition.ALLOW,
                            )
                        ],
                    )
                ],
            )

            self.assertEqual("admin_role", result.name())
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            self.assertEqual(self.METALAKE_ROLES_PATH, call_args.args[0])
            self.assertIsInstance(call_args.kwargs["json"], RoleCreateRequest)
            self.assertIs(ROLE_ERROR_HANDLER, call_args.kwargs["error_handler"])

    def test_create_role_empty_name_rejected(self):
        metalake = mock_base.mock_load_metalake()
        with self.assertRaises(IllegalArgumentException):
            metalake.create_role("")

    def test_create_role_already_exists(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=RoleAlreadyExistsException("role already exists"),
        ):
            with self.assertRaises(RoleAlreadyExistsException):
                metalake.create_role("admin_role")

    def test_get_role(self):
        metalake = mock_base.mock_load_metalake()
        role = _build_role_dto(props={"k": "v"})
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            result = metalake.get_role("admin_role")

            self.assertEqual("admin_role", result.name())
            self.assertEqual({"k": "v"}, result.properties())
            mock_get.assert_called_once()
            self.assertEqual(self.METALAKE_ROLE_PATH, mock_get.call_args.args[0])
            self.assertIs(
                ROLE_ERROR_HANDLER, mock_get.call_args.kwargs["error_handler"]
            )

    def test_get_role_not_found(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchRoleException("no such role"),
        ):
            with self.assertRaises(NoSuchRoleException):
                metalake.get_role("nonexistent")

    def test_delete_role(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(DropResponse(0, True).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ) as mock_delete:
            self.assertTrue(metalake.delete_role("admin_role"))

            mock_delete.assert_called_once()
            self.assertEqual(self.METALAKE_ROLE_PATH, mock_delete.call_args.args[0])
            self.assertIs(
                ROLE_ERROR_HANDLER, mock_delete.call_args.kwargs["error_handler"]
            )

    def test_delete_role_returns_false(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(DropResponse(0, False).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete",
            return_value=mock_resp,
        ):
            self.assertFalse(metalake.delete_role("admin_role"))

    def test_list_role_names(self):
        metalake = mock_base.mock_load_metalake()
        mock_resp = mock_base.mock_http_response(
            RoleNamesListResponse(0, ["role1", "role2"]).to_json()
        )

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            names = metalake.list_role_names()

            self.assertEqual(["role1", "role2"], names)
            mock_get.assert_called_once()
            self.assertEqual(self.METALAKE_ROLES_PATH, mock_get.call_args.args[0])
            self.assertIs(
                ROLE_ERROR_HANDLER, mock_get.call_args.kwargs["error_handler"]
            )

    def test_grant_roles_to_user(self):
        metalake = mock_base.mock_load_metalake()
        user = _build_user_dto(roles=["admin_role"])
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            result = metalake.grant_roles_to_user(["admin_role"], "alice")

            self.assertEqual("alice", result.name())
            self.assertEqual(["admin_role"], result.roles())
            mock_put.assert_called_once()
            self.assertEqual(
                self.PERMISSIONS_USER_GRANT_PATH, mock_put.call_args.args[0]
            )
            self.assertIsInstance(mock_put.call_args.kwargs["json"], RoleGrantRequest)
            self.assertIs(
                PERMISSION_ERROR_HANDLER,
                mock_put.call_args.kwargs["error_handler"],
            )

    def test_revoke_roles_from_user(self):
        metalake = mock_base.mock_load_metalake()
        user = _build_user_dto(roles=[])
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            result = metalake.revoke_roles_from_user(["admin_role"], "alice")

            self.assertEqual([], result.roles())
            mock_put.assert_called_once()
            self.assertEqual(
                self.PERMISSIONS_USER_REVOKE_PATH, mock_put.call_args.args[0]
            )
            self.assertIsInstance(mock_put.call_args.kwargs["json"], RoleRevokeRequest)
            self.assertIs(
                PERMISSION_ERROR_HANDLER,
                mock_put.call_args.kwargs["error_handler"],
            )

    def test_grant_roles_to_group(self):
        metalake = mock_base.mock_load_metalake()
        group = _build_group_dto(roles=["admin_role"])
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            result = metalake.grant_roles_to_group(["admin_role"], "engineers")

            self.assertEqual("engineers", result.name())
            self.assertEqual(["admin_role"], result.roles())
            mock_put.assert_called_once()
            self.assertEqual(
                self.PERMISSIONS_GROUP_GRANT_PATH, mock_put.call_args.args[0]
            )
            self.assertIs(
                PERMISSION_ERROR_HANDLER,
                mock_put.call_args.kwargs["error_handler"],
            )

    def test_revoke_roles_from_group(self):
        metalake = mock_base.mock_load_metalake()
        group = _build_group_dto(roles=[])
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            result = metalake.revoke_roles_from_group(["admin_role"], "engineers")

            self.assertEqual([], result.roles())
            mock_put.assert_called_once()
            self.assertEqual(
                self.PERMISSIONS_GROUP_REVOKE_PATH, mock_put.call_args.args[0]
            )
            self.assertIs(
                PERMISSION_ERROR_HANDLER,
                mock_put.call_args.kwargs["error_handler"],
            )

    def test_grant_privileges_to_role(self):
        metalake = mock_base.mock_load_metalake()
        sec_obj = SecurableObjectDTO(
            "my_catalog",
            MetadataObject.Type.CATALOG,
            [
                PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW),
                PrivilegeDTO(Privilege.Name.CREATE_SCHEMA, Privilege.Condition.ALLOW),
            ],
        )
        role = _build_role_dto(sec_objs=[sec_obj])
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            securable_obj = SecurableObjects.of_catalog(
                "my_catalog",
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
            result = metalake.grant_privileges_to_role(
                "admin_role",
                securable_obj,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )

            self.assertEqual("admin_role", result.name())
            mock_put.assert_called_once()
            self.assertEqual(
                self.PERMISSIONS_ROLE_GRANT_PATH, mock_put.call_args.args[0]
            )
            self.assertIsInstance(
                mock_put.call_args.kwargs["json"], PrivilegeGrantRequest
            )
            self.assertIs(
                PERMISSION_ERROR_HANDLER,
                mock_put.call_args.kwargs["error_handler"],
            )

    def test_revoke_privileges_from_role(self):
        metalake = mock_base.mock_load_metalake()
        role = _build_role_dto(sec_objs=[])
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            securable_obj = SecurableObjects.of_catalog(
                "my_catalog",
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
            result = metalake.revoke_privileges_from_role(
                "admin_role",
                securable_obj,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )

            self.assertEqual("admin_role", result.name())
            mock_put.assert_called_once()
            self.assertEqual(
                self.PERMISSIONS_ROLE_REVOKE_PATH, mock_put.call_args.args[0]
            )
            self.assertIs(
                PERMISSION_ERROR_HANDLER,
                mock_put.call_args.kwargs["error_handler"],
            )


class TestGravitinoClientRoleDelegates(unittest.TestCase):
    """Verify that GravitinoClient correctly delegates Role operations."""

    def _make_client(self):
        client = GravitinoClient.__new__(GravitinoClient)
        return client

    def test_client_create_role(self):
        client = self._make_client()
        role = _build_role_dto()
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.post",
                return_value=mock_resp,
            ),
        ):
            result = client.create_role("admin_role")
            self.assertEqual("admin_role", result.name())

    def test_client_get_role(self):
        client = self._make_client()
        role = _build_role_dto(props={"k": "v"})
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.get",
                return_value=mock_resp,
            ),
        ):
            result = client.get_role("admin_role")
            self.assertEqual({"k": "v"}, result.properties())

    def test_client_delete_role(self):
        client = self._make_client()
        mock_resp = mock_base.mock_http_response(DropResponse(0, True).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.delete",
                return_value=mock_resp,
            ),
        ):
            self.assertTrue(client.delete_role("admin_role"))

    def test_client_list_role_names(self):
        client = self._make_client()
        mock_resp = mock_base.mock_http_response(
            RoleNamesListResponse(0, ["role1", "role2"]).to_json()
        )
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.get",
                return_value=mock_resp,
            ),
        ):
            result = client.list_role_names()
            self.assertEqual(["role1", "role2"], result)

    def test_client_grant_roles_to_user(self):
        client = self._make_client()
        user = _build_user_dto(roles=["admin_role"])
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                return_value=mock_resp,
            ),
        ):
            result = client.grant_roles_to_user(["admin_role"], "alice")
            self.assertEqual(["admin_role"], result.roles())

    def test_client_revoke_roles_from_user(self):
        client = self._make_client()
        user = _build_user_dto(roles=[])
        mock_resp = mock_base.mock_http_response(UserResponse(0, user).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                return_value=mock_resp,
            ),
        ):
            result = client.revoke_roles_from_user(["admin_role"], "alice")
            self.assertEqual([], result.roles())

    def test_client_grant_roles_to_group(self):
        client = self._make_client()
        group = _build_group_dto(roles=["admin_role"])
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                return_value=mock_resp,
            ),
        ):
            result = client.grant_roles_to_group(["admin_role"], "engineers")
            self.assertEqual(["admin_role"], result.roles())

    def test_client_revoke_roles_from_group(self):
        client = self._make_client()
        group = _build_group_dto(roles=[])
        mock_resp = mock_base.mock_http_response(GroupResponse(0, group).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                return_value=mock_resp,
            ),
        ):
            result = client.revoke_roles_from_group(["admin_role"], "engineers")
            self.assertEqual([], result.roles())

    def test_client_grant_privileges_to_role(self):
        client = self._make_client()
        role = _build_role_dto()
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                return_value=mock_resp,
            ),
        ):
            securable_obj = SecurableObjects.of_catalog(
                "my_catalog",
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
            result = client.grant_privileges_to_role(
                "admin_role",
                securable_obj,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
            self.assertEqual("admin_role", result.name())

    def test_client_revoke_privileges_from_role(self):
        client = self._make_client()
        role = _build_role_dto()
        mock_resp = mock_base.mock_http_response(RoleResponse(0, role).to_json())
        with (
            patch.object(
                GravitinoClient,
                "get_metalake",
                return_value=mock_base.mock_load_metalake(),
            ),
            patch(
                "gravitino.utils.http_client.HTTPClient.put",
                return_value=mock_resp,
            ),
        ):
            securable_obj = SecurableObjects.of_catalog(
                "my_catalog",
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
            result = client.revoke_privileges_from_role(
                "admin_role",
                securable_obj,
                [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)],
            )
            self.assertEqual("admin_role", result.name())
