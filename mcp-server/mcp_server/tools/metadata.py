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


def load_metadata_tool(mcp: FastMCP):
    @mcp.tool(tags={"tag", "policy"})
    async def metadata_type_to_fullname_formats(ctx: Context) -> dict:
        """
        Get metadata type to full name formats.
        This tool provides the fullname format strings for different metadata types
        such as catalog, schema, table, model, topic, fileset, and column.

        Args:
            ctx (Context): Context object containing request metadata.
        Returns:
            dict: Dictionary containing formats for different metadata types.
            Key is the metadata type and value is the fullname format string.
        """

        return {
            "catalog": "{catalog}",
            "schema": "{catalog}.{schema}",
            "table": "{catalog}.{schema}.{table}",
            "model": "{catalog}.{schema}.{model}",
            "topic": "{catalog}.{schema}.{topic}",
            "fileset": "{catalog}.{schema}.{fileset}",
            "column": "{catalog}.{schema}.{table}.{column}",
        }
