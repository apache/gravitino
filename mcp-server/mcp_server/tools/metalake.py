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

from mcp_server.core.setting import METALAKE_TOOL_TAG


def load_metalake_tools(mcp: FastMCP):
    @mcp.tool(tags={METALAKE_TOOL_TAG})
    async def list_metalakes(ctx: Context) -> str:
        """
        Retrieve the list of metalakes the caller is allowed to access.

        A metalake is the top-level tenant boundary in Gravitino. Every other
        tool operates inside one: it uses the server's configured default
        metalake unless the call passes a `metalake` argument. Use this tool to
        discover which values that argument accepts - for example when a call
        failed because no metalake was specified, or when the user asks about a
        metalake other than the default.

        Args:
            ctx (Context): The request context.

        Returns:
            str: A JSON string containing the list of metalakes.

        Example Return Value:
            [
              {
                "name": "metalake_a",
                "comment": "Production metadata",
                "properties": {},
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-08-20T07:33:41.233089Z"
                }
              }
            ]

            name: The name of the metalake, i.e. the value to pass as the
                `metalake` argument of other tools.
            comment: A human-readable description of the metalake.
            properties: Metalake properties.
            audit: Metadata about the metalake's creation and modification.
        """
        # require_metalake=False: this is the tool an agent calls when it does
        # not know a metalake yet, so it must work with none configured.
        client = ctx.request_context.lifespan_context.rest_client(
            require_metalake=False
        )
        return await client.as_metalake_operation().get_list_of_metalakes()
