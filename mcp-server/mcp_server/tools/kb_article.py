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

from mcp_server.client.kb_article_operation import (
    ArticleAsset,
    KBArticleOperation,
)


def _articles(ctx: Context) -> KBArticleOperation:
    return (
        ctx.request_context.lifespan_context.rest_client().as_kb_article_operation()
    )


def load_kb_article_tools(mcp: FastMCP):
    """Register the read-only Knowledge Base Article tools."""
    tool = mcp.tool(
        tags={"kb_article"},
        annotations={
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
        },
    )

    @tool
    async def search_kb_articles(
        ctx: Context,
        q: str | None = None,
        domain: str | None = None,
        owner: str | None = None,
        *,
        limit: int = 20,
        offset: int = 0,
        term: str | None = None,
        asset: ArticleAsset | None = None,
    ) -> str:
        """Browse summaries of readable PUBLISHED Articles, or search titles, synonyms and bodies.

        Start with list_kb_articles_for_term or list_kb_articles_for_object when
        the target is known. Otherwise use list_of_domains and browse a Domain
        with q omitted. Add q within that Domain if more discovery is needed.
        Keep the scope of an empty result. Filters can be combined. Supply an owner
        as user:<name> or group:<name>, a Term by name, and an asset as type,
        namespace components and name.
        When hasMore is true, advance offset by the number of Articles returned.
        Read selected results with get_kb_article.
        """
        return await _articles(ctx).search_kb_articles(
            q, domain, owner, limit=limit, offset=offset, term=term, asset=asset
        )

    @tool
    async def get_kb_article(
        ctx: Context, name: str, limit: int = 20, offset: int = 0
    ) -> str:
        """Read a PUBLISHED Article for its explanation and visible bindings.

        The body contains the stored MARKDOWN or HTML source and its format.
        Increase offset by limit when termsHasMore or objectsHasMore is true.
        Follow a visible Term by its name using get_term when available.
        For a TABLE binding, pass namespace[0] as catalog_name, namespace[1]
        as schema_name, and name as table_name to get_table_metadata_details.
        For a SEMANTIC_MODEL binding, use the same catalog_name and schema_name
        and pass name as semantic_model_name to load_semantic_model.
        Each target requires its own access. A Term and an asset bound to the
        same Article have independent relationships with it.
        """
        return await _articles(ctx).get_kb_article(name, limit, offset)

    @tool
    async def list_kb_articles_for_term(
        ctx: Context, term: str, limit: int = 20, offset: int = 0
    ) -> str:
        """List readable PUBLISHED Article summaries directly bound to a Glossary Term.

        Use the exact Term name. When hasMore is true, advance offset by the
        number of Articles returned. Read a selected result with get_kb_article.
        """
        return await _articles(ctx).list_kb_articles_for_term(
            term, limit, offset
        )

    @tool
    async def list_kb_articles_for_object(
        ctx: Context,
        object_type: str,
        namespace: list[str],
        name: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> str:
        """List readable PUBLISHED Article summaries directly bound to an asset.

        Pass its type, ordered namespace components, and exact leaf name. For a
        TABLE, namespace is [catalog, schema]. For a COLUMN, include the table
        as the third component. For a CATALOG, pass []. Preserve literal dots
        in each component. When hasMore is true, advance offset by the number
        of Articles returned.
        """
        return await _articles(ctx).list_kb_articles_for_object(
            object_type, namespace, name, limit=limit, offset=offset
        )
