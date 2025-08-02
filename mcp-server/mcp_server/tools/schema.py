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


def load_schema_tools(mcp: FastMCP):
    @mcp.tool(
        name="get_list_of_schemas",
        description="Get a list of schemas, filtered by catalog it belongs to.",
    )
    def get_list_of_schemas(ctx: Context, catalog_name: str):
        """
        Get a list of schemas, filtered by catalog it belongs to.

        Args:
            catalog_name (str):  name of the catalog
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_schema_operation().get_list_of_schemas(catalog_name)
