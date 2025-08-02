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

def load_table_tools(mcp: FastMCP):
    @mcp.tool(
        name="get_list_of_tables",
        description="Get a list of tables, filtered by catalog and schema it belongs to.",
    )
    def get_list_of_tables(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Get a list of tables, optionally filtered by database it belongs to.
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_table_operation().get_list_of_tables(
            catalog_name, schema_name
        )

    @mcp.tool(
        name="get_table_detail_by_name",
        description="Get table detail information by catalog_name, schema_name and table_name.",
    )
    def get_table_metadata_details(
        ctx: Context, catalog_name: str, schema_name: str, table_name: str
    ):
        """
        Get table detail information by catalog_name, schema_name and table_name.
        Args:
            catalog_name (str):  name of the catalog
            schema_name (str):  name of the schema
            table_name (str):  name of the table
        Returns:
            str: A json str include table details including columns, comment, partitioning, distribution, sort order,etc
        """
        connector = ctx.request_context.lifespan_context.connector()
        return connector.as_table_operation().load_table(
            catalog_name, schema_name, table_name
        )
