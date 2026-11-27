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


"""Ontology and mapping tools exposed to MCP clients."""

from __future__ import annotations

from fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations


def load_ontology_tools(mcp: FastMCP):
    """Registers ontology lifecycle tools."""

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_ontologies(ctx: Context) -> str:
        """List visible ontology names."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().list_ontologies()

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def load_ontology(ctx: Context, name: str) -> str:
        """Load an ontology and its definition."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().load_ontology(name)

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=False, idempotentHint=False
        ),
    )
    async def create_ontology(
        ctx: Context,
        name: str,
        definition: dict,
        comment: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        """Create an ontology from its native definition."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().create_ontology(
            name, definition, comment, properties
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=True, idempotentHint=False
        ),
    )
    async def alter_ontology(
        ctx: Context, name: str, updates: list[dict]
    ) -> str:
        """Alter an ontology with an ordered batch of changes."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().alter_ontology(
            name, updates
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=True, idempotentHint=False
        ),
    )
    async def drop_ontology(ctx: Context, name: str) -> str:
        """Drop an ontology that has no live mappings."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().drop_ontology(name)

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_ontology_mappings(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
    ) -> str:
        """List mapping identifiers under a semantic model."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().list_ontology_mappings(
            catalog_name, schema_name, semantic_model_name
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def load_ontology_mapping(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
    ) -> str:
        """Load a mapping and its definition."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().load_ontology_mapping(
            catalog_name, schema_name, semantic_model_name, name
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=False, idempotentHint=False
        ),
    )
    # pylint: disable=too-many-positional-arguments
    async def create_ontology_mapping(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
        ontology: str,
        definition: dict,
        comment: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        """Create a mapping bound to an ontology."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().create_ontology_mapping(
            catalog_name,
            schema_name,
            semantic_model_name,
            name,
            ontology,
            definition,
            comment,
            properties,
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=True, idempotentHint=False
        ),
    )
    # pylint: disable=too-many-positional-arguments
    async def alter_ontology_mapping(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
        updates: list[dict],
    ) -> str:
        """Alter a mapping with an ordered batch of changes."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().alter_ontology_mapping(
            catalog_name, schema_name, semantic_model_name, name, updates
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=True, idempotentHint=False
        ),
    )
    async def drop_ontology_mapping(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        name: str,
    ) -> str:
        """Drop a mapping."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().drop_ontology_mapping(
            catalog_name, schema_name, semantic_model_name, name
        )

    @mcp.tool(
        tags={"ontology"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_mappings_by_ontology(ctx: Context, name: str) -> str:
        """List mappings of an ontology whose semantic models the caller can load."""
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_ontology_operation().list_mappings_by_ontology(
            name
        )
