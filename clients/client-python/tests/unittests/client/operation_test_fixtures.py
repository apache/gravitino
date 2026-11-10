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

from unittest.mock import patch

from gravitino.client.gravitino_client import GravitinoClient
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.group_dto import GroupDTO
from gravitino.dto.authorization.role_dto import RoleDTO
from gravitino.dto.authorization.user_dto import UserDTO
from tests.unittests import mock_base


def build_default_audit() -> AuditDTO:
    return mock_base.build_audit_info()


def build_admin_audit() -> AuditDTO:
    return AuditDTO(_creator="admin", _create_time="2024-01-01T00:00:00Z")


def build_user_dto(
    name: str = "alice",
    roles: list | None = None,
    audit: AuditDTO | None = None,
) -> UserDTO:
    return (
        UserDTO.builder()
        .with_name(name)
        .with_roles(roles if roles is not None else [])
        .with_audit(audit or build_default_audit())
        .build()
    )


def build_group_dto(
    name: str = "engineers",
    roles: list | None = None,
    audit: AuditDTO | None = None,
) -> GroupDTO:
    return (
        GroupDTO.builder()
        .with_name(name)
        .with_roles(roles if roles is not None else [])
        .with_audit(audit or build_default_audit())
        .build()
    )


def build_role_dto(
    name: str = "admin_role",
    props: dict | None = None,
    sec_objs: list | None = None,
    audit: AuditDTO | None = None,
) -> RoleDTO:
    return (
        RoleDTO.builder()
        .with_name(name)
        .with_properties(props)
        .with_securable_objects(sec_objs or [])
        .with_audit(audit or build_admin_audit())
        .build()
    )


def make_gravitino_client() -> GravitinoClient:
    return GravitinoClient.__new__(GravitinoClient)


def mock_get_metalake():
    return patch.object(
        GravitinoClient,
        "get_metalake",
        return_value=mock_base.mock_load_metalake(),
    )
