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

from typing import Any, Dict, Sequence

import mcp.types as mt
from fastmcp.server.middleware.middleware import (
    CallNext,
    Middleware,
    MiddlewareContext,
)
from fastmcp.tools.base import Tool, ToolResult

from mcp_server.core.context import (
    METALAKE_ARGUMENT,
    reset_request_metalake,
    set_request_metalake,
)

# Tools that never resolve a metalake, so advertising the argument on them
# would offer the agent a knob that does nothing. `list_metalakes` is the
# discovery tool itself (its whole point is working without a metalake) and
# `metadata_type_to_fullname_formats` is pure computation that never calls
# Gravitino. A tool missing from this set only gets a harmless no-op argument.
TOOLS_WITHOUT_METALAKE = frozenset(
    {"list_metalakes", "metadata_type_to_fullname_formats"}
)

_METALAKE_ARGUMENT_DESCRIPTION = (
    "Metalake to operate on. Omit to use the server's configured default "
    "metalake. Call 'list_metalakes' to discover which metalakes are "
    "available to you."
)


def _schema_with_metalake(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Return ``parameters`` with an optional ``metalake`` property added.

    Copied rather than mutated so the registered Tool objects keep the schema
    their functions actually declare; the argument exists only on the wire.
    ``required`` is deliberately left alone - omitting the argument is what
    every single-metalake deployment does.
    """
    schema = dict(parameters)
    properties = dict(schema.get("properties") or {})
    # Never shadow a parameter a tool declares itself.
    if METALAKE_ARGUMENT in properties:
        return parameters
    properties[METALAKE_ARGUMENT] = {
        "type": "string",
        "description": _METALAKE_ARGUMENT_DESCRIPTION,
    }
    schema["properties"] = properties
    return schema


class MetalakeArgumentMiddleware(Middleware):
    """Lets any tool call name the metalake it operates on.

    Every tool gains an optional ``metalake`` argument without declaring it:
    this middleware advertises it in each tool's input schema, strips it from
    the incoming arguments before the tool function runs, and publishes it for
    ``GravitinoContext.rest_client()`` to resolve against.

    The value lives in a context variable for the duration of one tool call
    only, so no metalake state is carried between calls or shared between
    server replicas.
    """

    async def on_list_tools(
        self,
        context: MiddlewareContext[mt.ListToolsRequest],
        call_next: CallNext[mt.ListToolsRequest, Sequence[Tool]],
    ) -> Sequence[Tool]:
        tools = await call_next(context)
        return [
            (
                tool
                if tool.name in TOOLS_WITHOUT_METALAKE
                else tool.model_copy(
                    update={
                        "parameters": _schema_with_metalake(tool.parameters)
                    }
                )
            )
            for tool in tools
        ]

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        arguments = context.message.arguments
        # Popped so the tool function never sees an argument it cannot accept.
        metalake = (
            arguments.pop(METALAKE_ARGUMENT, "")
            if isinstance(arguments, dict)
            else ""
        )
        token = set_request_metalake(metalake)
        try:
            return await call_next(context)
        finally:
            # Without this the metalake would leak into the next call served on
            # this context, turning per-call plumbing into implicit state.
            reset_request_metalake(token)
