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

from fastmcp import Context, FastMCP

_READONLY_TOOL_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
}


def load_domain_tools(mcp: FastMCP):
    """Register only read-only domain tools."""

    @mcp.tool(
        tags={"domain"},
        annotations=_READONLY_TOOL_ANNOTATIONS,
    )
    async def list_of_domains(ctx: Context, details: bool = True) -> str:
        """List domains; details includes backingTag, status, parent and audit.

        Returns a JSON string; empty lists are returned as [].
        Authorization and errors are enforced by the Gravitino server.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_domain_operation().list_of_domains(details)

    @mcp.tool(
        tags={"domain"},
        annotations=_READONLY_TOOL_ANNOTATIONS,
    )
    async def get_domain_by_name(ctx: Context, domain_name: str) -> str:
        """Load a domain snapshot by name.

        Returns a JSON string containing the complete domain snapshot.
        Authorization and errors are enforced by the Gravitino server.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_domain_operation().get_domain_by_name(
            domain_name
        )

    @mcp.tool(
        tags={"domain"},
        annotations=_READONLY_TOOL_ANNOTATIONS,
    )
    async def list_subdomains(
        ctx: Context, domain_name: str, details: bool = True
    ) -> str:
        """List immediate subdomains, as names or complete snapshots.

        Returns a JSON string; empty lists are returned as [].
        Authorization and errors are enforced by the Gravitino server.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_domain_operation().list_subdomains(
            domain_name, details
        )

    @mcp.tool(
        tags={"domain"},
        annotations=_READONLY_TOOL_ANNOTATIONS,
    )
    async def list_domain_members(
        ctx: Context,
        domain_name: str,
        types: list[str] | None = None,
        domains: list[str] | None = None,
        name_contains: str | None = None,
        expand: bool = False,
    ) -> str:
        """List domain members and all applicable relations.

        By default, lists objects that carry the domain's backing tag themselves,
        including catalogs and schemas; a CATALOG or SCHEMA member includes all
        objects under it and is listed once. expand=true instead expands catalogs
        and schemas into the recorded leaf objects under them, with INHERITED
        relations; use it to enumerate every table, view, fileset, topic, model or
        semantic model in a domain.
        types filters object types (for example TABLE, TOPIC, CATALOG or SCHEMA;
        CATALOG and SCHEMA are not allowed with expand=true).
        domains filters source domain names. name_contains matches a case-insensitive
        substring of the full metadata object name. Blank values omit this filter.

        Returns a JSON string; empty lists are returned as [].
        Authorization and errors are enforced by the Gravitino server.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_domain_operation().list_domain_members(
            domain_name, types, domains, name_contains, expand
        )

    @mcp.tool(
        tags={"domain"},
        annotations=_READONLY_TOOL_ANNOTATIONS,
    )
    async def list_domains_for_metadata(
        ctx: Context,
        metadata_full_name: str,
        metadata_type: str,
        details: bool = True,
    ) -> str:
        """List domains for a leaf metadata object.

        metadata_type supports table, fileset, topic, model, view and semantic_model.
        metadata_full_name is the qualified object name, such as catalog.schema.table.
        details preserves all relations and their sources; false returns names only.

        Returns a JSON string; empty lists are returned as [].
        Authorization and errors are enforced by the Gravitino server.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_domain_operation().list_domains_for_metadata(
            metadata_full_name, metadata_type, details
        )
