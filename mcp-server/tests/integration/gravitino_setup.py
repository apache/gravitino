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

"""Provision Gravitino metadata and authorization fixtures for the integration test.

All requests are issued as the service admin using Gravitino simple
authentication (``Authorization: Basic base64(user:dummy)``). The fixture
creates a metalake with two model catalogs, a non-admin user ``bob``, and a role
that grants ``bob`` access to only one of the two catalogs. This produces a
visibly different authorization slice between the admin and ``bob`` principals.
"""

import base64

import httpx


def basic_auth_header(user: str) -> str:
    """Build a Gravitino simple-auth header value for ``user``."""
    credential = base64.b64encode(f"{user}:dummy".encode("utf-8")).decode(
        "ascii"
    )
    return f"Basic {credential}"


class GravitinoFixture:  # pylint: disable=too-many-instance-attributes
    """Sets up metalake/catalogs/user/role/grant via the Gravitino REST API."""

    def __init__(  # pylint: disable=too-many-positional-arguments,too-many-arguments
        self,
        gravitino_uri: str,
        metalake: str,
        admin_user: str = "admin",
        granted_user: str = "bob",
        catalog_allowed: str = "cat_allowed",
        catalog_denied: str = "cat_denied",
        role_name: str = "reader_role",
    ):
        self.gravitino_uri = gravitino_uri.rstrip("/")
        self.metalake = metalake
        self.admin_user = admin_user
        self.granted_user = granted_user
        self.catalog_allowed = catalog_allowed
        self.catalog_denied = catalog_denied
        self.role_name = role_name
        self._client = httpx.Client(
            base_url=self.gravitino_uri,
            headers={"Authorization": basic_auth_header(admin_user)},
            timeout=30.0,
        )

    def _post(self, path: str, body: dict) -> httpx.Response:
        response = self._client.post(path, json=body)
        response.raise_for_status()
        return response

    def _put(self, path: str, body: dict) -> httpx.Response:
        response = self._client.put(path, json=body)
        response.raise_for_status()
        return response

    def provision(self) -> None:
        """Create all metadata and authorization fixtures.

        Not idempotent: every step raises on a non-2xx response, so re-running
        against an already-provisioned metalake fails (e.g. HTTP 409). Expects a
        clean Gravitino instance.
        """
        self._create_metalake()
        self._create_model_catalog(self.catalog_allowed)
        self._create_model_catalog(self.catalog_denied)
        self._add_user(self.granted_user)
        self._create_reader_role()
        self._grant_role_to_user()

    def _create_metalake(self) -> None:
        self._post(
            "/api/metalakes",
            {
                "name": self.metalake,
                "comment": "MCP authz integration test metalake",
                "properties": {},
            },
        )

    def _create_model_catalog(self, name: str) -> None:
        self._post(
            f"/api/metalakes/{self.metalake}/catalogs",
            {
                "name": name,
                "type": "MODEL",
                "provider": "model",
                "comment": "model catalog for authz test",
                "properties": {},
            },
        )

    def _add_user(self, user: str) -> None:
        self._post(
            f"/api/metalakes/{self.metalake}/users",
            {"name": user},
        )

    def _create_reader_role(self) -> None:
        # Grant bob USE_CATALOG on the allowed catalog only.
        self._post(
            f"/api/metalakes/{self.metalake}/roles",
            {
                "name": self.role_name,
                "properties": {},
                "securableObjects": [
                    {
                        "fullName": self.catalog_allowed,
                        "type": "CATALOG",
                        "privileges": [
                            {"name": "USE_CATALOG", "condition": "ALLOW"}
                        ],
                    }
                ],
            },
        )

    def _grant_role_to_user(self) -> None:
        self._put(
            f"/api/metalakes/{self.metalake}/permissions"
            f"/users/{self.granted_user}/grant/",
            {"roleNames": [self.role_name]},
        )

    def close(self) -> None:
        self._client.close()
