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

from mcp_server.client.domain_operation import DomainOperation
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
)


class PlainRESTClientDomainOperation(DomainOperation):
    """Read domain snapshots without projecting away governance information."""

    def __init__(self, metalake_name: str, rest_client):
        self._metalake_name = metalake_name
        self._rest_client = rest_client

    @property
    def _path(self) -> str:
        # Metalake discovery can construct a client without a selected metalake.
        # Validate the scope only when a domain request actually needs it.
        return (
            f"/api/metalakes/{encode_path_segment(self._metalake_name)}/domains"
        )

    @property
    def _object_path(self) -> str:
        return (
            f"/api/metalakes/{encode_path_segment(self._metalake_name)}/objects"
        )

    async def _list(self, path: str, details: bool) -> str:
        response = await self._rest_client.get(
            path, params={"details": str(details).lower()}
        )
        return self._extract(response, "domains" if details else "names", [])

    async def list_of_domains(self, details: bool = True) -> str:
        return await self._list(self._path, details)

    async def get_domain_by_name(self, domain_name: str) -> str:
        response = await self._rest_client.get(
            f"{self._path}/{encode_path_segment(domain_name)}"
        )
        return self._extract(response, "domain", {})

    async def list_subdomains(
        self, domain_name: str, details: bool = True
    ) -> str:
        return await self._list(
            f"{self._path}/{encode_path_segment(domain_name)}/subdomains",
            details,
        )

    async def list_domain_members(
        self,
        domain_name: str,
        types: list[str] | None = None,
        domains: list[str] | None = None,
        name_contains: str | None = None,
    ) -> str:
        # Repeated query parameters preserve commas and other characters in domain names.
        params = []
        if types:
            params.append(("types", ",".join(types)))
        if domains:
            params.extend(("domains", domain) for domain in domains)
        if name_contains is not None:
            params.append(("nameContains", name_contains))

        response = await self._rest_client.get(
            f"{self._path}/{encode_path_segment(domain_name)}/objects",
            params=params,
        )
        return self._extract(response, "members", [])

    async def list_domains_for_metadata(
        self, metadata_full_name: str, metadata_type: str, details: bool = True
    ) -> str:
        return await self._list(
            f"{self._object_path}/{encode_path_segment(metadata_type)}"
            f"/{encode_path_segment(metadata_full_name)}/domains",
            details,
        )

    @staticmethod
    def _extract(response, field: str, default) -> str:
        # Preserve structured Gravitino errors before checking HTTP status.
        # Gateways may return JSON without a Gravitino error code; these must
        # never turn authentication failures into successful empty results.
        content = extract_content_from_response(response, field, default)
        response.raise_for_status()
        return content
