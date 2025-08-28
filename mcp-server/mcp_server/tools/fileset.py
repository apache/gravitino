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


def load_fileset_tools(mcp: FastMCP):
    @mcp.tool(tags={"fileset"})
    async def list_of_filesets(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Retrieve a list of filesets within a specific catalog and schema.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter filesets by.
            schema_name (str): The name of the schema to filter filesets by.

        Returns:
            str: A JSON string representing an array of fileset identifiers.

        Example Return Value:
            [
              {
                "namespace": [
                  "test",
                  "fileset_catalog",
                  "fileset_schema1"
                ],
                "name": "fileset1"
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_fileset_operation().list_of_filesets(
            catalog_name, schema_name
        )

    @mcp.tool(tags={"fileset"})
    async def load_fileset(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
    ):
        """
        Load detailed information of a specific fileset.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the fileset.
            schema_name (str): The name of the schema containing the fileset.
            fileset_name (str): The name of the fileset to load.

        Returns:
            str: A JSON string containing full metadata of the specified fileset.

        Example Return Value:
            {
              "name": "fileset1",
              "comment": "",
              "type": "managed",
              "storageLocations": {
                "location1": "file:/tmp/fileset1"
              },
              "properties": {
                "default-location-name": "location1"
              },
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-07T07:52:56.656313Z"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_fileset_operation().load_fileset(
            catalog_name, schema_name, fileset_name
        )

    # pylint:disable=too-many-positional-arguments
    @mcp.tool(tags={"fileset"})
    async def list_files_in_fileset(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
        location_name: str,
        sub_path: str = "/",
    ):
        """
        List files in a specific fileset.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the fileset.
            schema_name (str): The name of the schema containing the fileset.
            fileset_name (str): The name of the fileset to list files from.
            location_name (str): The name of the location within the fileset.
            sub_path (str): Sub-path within the fileset to list files from (default is root "/").

        Returns:
            str: A JSON string containing a list of files in the specified fileset.

        Example Return Value:
            [
              {
                "name": "2.txt",
                "isDir": false,
                "size": 2,
                "lastModified": 1754911672329,
                "path": "/fileset/fileset_catalog/fileset_schema1/fileset1/"
              },
              {
                "name": "1.txt",
                "isDir": false,
                "size": 2,
                "lastModified": 1754911668512,
                "path": "/fileset/fileset_catalog/fileset_schema1/fileset1/"
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_fileset_operation().list_files_in_fileset(
            catalog_name, schema_name, fileset_name, location_name, sub_path
        )
