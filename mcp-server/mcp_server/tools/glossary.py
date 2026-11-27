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

from mcp_server.client.glossary_operation import GlossaryOperation


def _glossary(ctx: Context) -> GlossaryOperation:
    return (
        ctx.request_context.lifespan_context.rest_client().as_glossary_operation()
    )


def load_glossary_tools(mcp: FastMCP):
    """Register the read-only glossary tools."""
    tool = mcp.tool(
        tags={"glossary"},
        annotations={
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
        },
    )

    @tool
    async def resolve_business_term(
        ctx: Context,
        words: list[str],
        domain: str | None = None,
        include_bindings: bool = True,
    ) -> str:
        """Resolve business phrases to readable APPROVED terms by exact name or synonym.

        Keep each phrase whole and in the singular form. Leave out time ranges, numbers
        and query words. Use domain to narrow the search. For ambiguous matches, use
        the domains to choose a term or ask the user. Ask for clarification on unmatched
        words. They have no readable approved exact match in this query.

        For a TABLE or COLUMN binding, pass catalog, schema and name to
        get_table_metadata_details as catalog_name, schema_name and table_name.
        For a metric, pass them to load_semantic_model as catalog_name, schema_name
        and semantic_model_name, then find the named metric. Use the same metalake.
        These reads require their own privileges. Cite the terms and definitions you used.

        When bindings_truncated is true, call list_metadata_by_term for the full list.
        On LookupTooBroadException, retry with the same domain and include_bindings=false.
        This omits binding fields. Use list_metadata_by_term for a selected term.
        If the definition-only lookup also exceeds its budget, ask for a narrower query.
        """
        return await _glossary(ctx).resolve_business_term(
            words, domain, include_bindings
        )

    @tool
    async def get_term(ctx: Context, term: str) -> str:
        """Get an APPROVED term's definition, synonyms, visible domains and approved relations.

        Check a term discovered by a generic metadata tool here before using its definition.
        """
        return await _glossary(ctx).get_term(term)

    @tool
    async def list_terms(
        ctx: Context,
        domain: str | None = None,
        name_contains: str | None = None,
    ) -> str:
        """Browse readable APPROVED definitions by domain and case-insensitive name substring."""
        return await _glossary(ctx).list_terms(domain, name_contains)

    @tool
    async def get_term_hierarchy(
        ctx: Context, term: str, direction: str = "NARROWER"
    ) -> str:
        """List readable APPROVED descendants (NARROWER) or ancestors (BROADER) of an approved term."""
        return await _glossary(ctx).get_term_hierarchy(term, direction)

    @tool
    async def list_metadata_by_term(
        ctx: Context,
        term: str,
        kind: str | None = None,
        types: list[str] | None = None,
    ) -> str:
        """List all visible object and metric bindings of an APPROVED term.

        Filter by kind (IMPLEMENTED_BY or USED_BY) and object types such as TABLE,
        COLUMN or SEMANTIC_MODEL. Reading the bound data requires the usual privileges.
        """
        return await _glossary(ctx).list_metadata_by_term(term, kind, types)

    @tool
    async def list_terms_for_metadata(
        ctx: Context, metadata_full_name: str, metadata_type: str
    ) -> str:
        """List readable APPROVED terms for a qualified metadata object.

        Tables include visible column terms and semantic models include visible metric
        terms. Other objects, including views, return their direct terms.
        """
        return await _glossary(ctx).list_terms_for_metadata(
            metadata_full_name, metadata_type
        )
