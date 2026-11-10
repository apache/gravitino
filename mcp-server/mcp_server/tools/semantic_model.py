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

from typing import Literal, Optional

from fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations


def load_semantic_model_tools(mcp: FastMCP):
    @mcp.tool(
        tags={"semantic_model"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_of_semantic_models(
        ctx: Context, catalog_name: str, schema_name: str
    ) -> str:
        """List the Semantic Models the caller can access in a schema.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.

        Returns:
            A JSON array of identifiers, each with a namespace
            [metalake, catalog, schema] and a name.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return (
            await client.as_semantic_model_operation().list_of_semantic_models(
                catalog_name, schema_name
            )
        )

    @mcp.tool(
        tags={"semantic_model"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_metrics(
        ctx: Context, catalog_name: str, schema_name: str
    ) -> str:
        """List visible metric identifiers across the Semantic Models in a schema.

        Gravitino applies containing-model visibility and sorts by model
        identifier, then case-sensitive metric name. Models without metrics
        contribute no entries.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.

        Returns:
            A JSON array of identifiers with namespace
            [metalake, catalog, schema, semanticModel] and the metric name.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().list_metrics(
            catalog_name, schema_name
        )

    @mcp.tool(
        tags={"semantic_model"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_metric_infos(
        ctx: Context, catalog_name: str, schema_name: str
    ) -> str:
        """List complete visible metrics across the Semantic Models in a schema.

        Uses the same visibility and ordering as list_metrics. A failed request
        raises an error rather than returning a partial list.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.

        Returns:
            A JSON array of objects with namespace
            [metalake, catalog, schema, semanticModel] and a complete metric
            containing name, expression, and any description, datatype,
            aiContext, and customExtensions. JSON number precision is preserved.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().list_metric_infos(
            catalog_name, schema_name
        )

    @mcp.tool(
        tags={"semantic_model"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def load_semantic_model(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
    ) -> str:
        """Load a Semantic Model's metadata and complete semantic definition.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.
            semantic_model_name: Name of the Semantic Model.

        Returns:
            A JSON object containing name, comment, definition, properties,
            and audit information in Gravitino's native REST format.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().load_semantic_model(
            catalog_name, schema_name, semantic_model_name
        )

    @mcp.tool(
        tags={"semantic_model"},
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    )
    async def list_semantic_model_datasets(
        ctx: Context,
        source_type: Literal["table", "view"],
        catalog_name: str,
        schema_name: str,
        source_name: str,
    ) -> str:
        """List visible Semantic Model datasets directly associated with a source.

        Requires access to the source metadata. Results may span catalogs and
        schemas in the selected metalake and include only models the caller can
        load. Preserves the server's component-wise, case-sensitive UTF-16 order.

        Source catalog, schema, and table/view names must not contain literal
        dots. The REST source lookup route uses catalog.schema.name without
        component escaping; these inputs are rejected before sending a request.

        Associations reflect the latest definition write, not model execution
        validity or transitive dependencies. An empty result does not establish
        that changing or deleting the source is safe.

        Args:
            ctx: The request context.
            source_type: The source kind, either table or view.
            catalog_name: Name of the source catalog.
            schema_name: Name of the source schema.
            source_name: Name of the source table or view.

        Returns:
            A JSON array of identifiers with namespace
            [metalake, catalog, schema, semanticModel] and the exact dataset
            name, including any literal dots. No visible matches returns [];
            failed requests raise tool errors.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().list_semantic_model_datasets(
            source_type, catalog_name, schema_name, source_name
        )

    @mcp.tool(tags={"semantic_model"})
    # pylint: disable=too-many-positional-arguments
    async def create_semantic_model(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        name: str,
        definition: dict,
        comment: Optional[str] = None,
        properties: Optional[dict[str, str]] = None,
    ) -> str:
        """Create a schema-scoped Semantic Model.

        Gravitino validates the definition, source access, and authorization.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.
            name: Name of the Semantic Model to create.
            definition: Native REST definition with a non-empty datasets
                array and optional relationships, metrics, aiContext, and
                customExtensions. Use camelCase fields. A minimal example:
                {"datasets": [{"name": "orders", "source": {
                    "namespace": ["sales", "mart"], "name": "orders"
                }, "fields": []}]}. Source namespaces contain the source
                catalog and schema within the selected metalake.
            comment: Optional description.
            properties: Optional Gravitino properties with string values.

        Returns:
            The created Semantic Model as a JSON object.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().create_semantic_model(
            catalog_name, schema_name, name, definition, comment, properties
        )

    @mcp.tool(tags={"semantic_model"})
    async def alter_semantic_model(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        updates: list[dict],
    ) -> str:
        """Apply an ordered, non-empty list of updates atomically.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.
            semantic_model_name: Current name of the Semantic Model.
            updates: Native REST operations selected by @type:
                rename (newName), updateComment (newComment, null to clear),
                setProperty (property, value), removeProperty (property),
                or replaceDefinition (definition). Example:
                [{"@type": "setProperty", "property": "domain",
                  "value": "sales"}]. replaceDefinition supplies the entire
                definition and revalidates source access.

        Returns:
            The altered Semantic Model as a JSON object.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().alter_semantic_model(
            catalog_name, schema_name, semantic_model_name, updates
        )

    @mcp.tool(tags={"semantic_model"})
    async def drop_semantic_model(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
    ) -> str:
        """Drop a Semantic Model's metadata without deleting source data.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.
            semantic_model_name: Name of the Semantic Model to drop.

        Returns:
            A JSON boolean indicating whether the Semantic Model was dropped.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().drop_semantic_model(
            catalog_name, schema_name, semantic_model_name
        )

    @mcp.tool(tags={"semantic_model"})
    # pylint: disable=too-many-positional-arguments
    async def plan_semantic_query(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        semantic_model_name: str,
        metrics: Optional[list[str]] = None,
        dimensions: Optional[list[str]] = None,
        filters: Optional[list[dict]] = None,
        limit: Optional[int] = None,
    ) -> str:
        """Resolve a Semantic Model query into a dialect-neutral logical plan.

        The server resolves the query against the current committed definition
        without rendering SQL or executing anything. The returned plan names the
        sources, joins, selections, filters, grouping set and row limit, and
        records the Semantic Model version it was resolved against. A plan is
        not authorization to execute it.

        Args:
            ctx: The request context.
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.
            semantic_model_name: Name of the Semantic Model to query.
            metrics: Optional metric names to select.
            dimensions: Optional dimension names to select.
            filters: Optional filters, each a JSON object with field,
                operator (eq, ne, gt, ge, lt, or le), and value strings.
            limit: Optional non-negative row limit.

        Returns:
            A JSON object containing the resolved plan and any warnings.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_semantic_model_operation().plan_semantic_query(
            catalog_name,
            schema_name,
            semantic_model_name,
            metrics,
            dimensions,
            filters,
            limit,
        )
