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


def load_model_tools(mcp: FastMCP):
    @mcp.tool(tags={"model"})
    async def list_of_models(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Retrieve a list of models within a specific catalog and schema.
        This function returns a JSON-formatted string containing model identifiers
        filtered by the specified catalog and schema names.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter models by.
            schema_name (str): The name of the schema to filter models by.

        Returns:
            str: A JSON string representing an array of model objects with the following structure:
            - namespace: An array of strings representing the hierarchical namespace of the model.
            - name: The name of the model.

        Example Return Value:
            [
              {
                "namespace": [
                  "test",
                  "model_catalog",
                  "schema1"
                ],
                "name": "model1"
              },
              {
                "namespace": [
                  "test",
                  "model_catalog",
                  "schema1"
                ],
                "name": "model2"
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_model_operation().list_of_models(
            catalog_name, schema_name
        )

    @mcp.tool(tags={"model"})
    async def load_model(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        model_name: str,
    ):
        """
        Load detailed information of a specific model.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter models by.
            schema_name (str): The name of the schema to filter models by.
            model_name (str): The name of the model to load.

        Returns:
            str: A JSON string containing full model metadata.

        Example Return Value:
            {
              "name": "model1",
              "comment": "",
              "properties": {},
              "latestVersion": 0,
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-11T06:50:04.403532Z"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_model_operation().load_model(
            catalog_name, schema_name, model_name
        )

    @mcp.tool(tags={"model"})
    async def list_model_versions(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        model_name: str,
    ):
        """
        List all versions of a specific model.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter models by.
            schema_name (str): The name of the schema to filter models by.
            model_name (str): The name of the model to list versions for.

        Returns:
            str: A JSON string containing model version information. The string
                represents an array of version information.

        Example Return Value:
            [
              {
                "version": 1,
                "comment": "this is version2",
                "aliases": [
                  "alias2"
                ],
                "uri": "/tmp/version2",
                "properties": {},
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-08-11T06:56:06.340031Z"
                }
              }
            ]

        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_model_operation().list_model_versions(
            catalog_name, schema_name, model_name
        )

    @mcp.tool(tags={"model"})
    async def load_model_version(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        version: int,
    ):
        """
        Load detailed information of a specific version of a model.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter models by.
            schema_name (str): The name of the schema to filter models by.
            model_name (str): The name of the model to load.
            version (int): Version identifier of the model.

        Returns:
            str: A JSON string containing full model version metadata.

        Example Return Value:
            {
              "version": 1,
              "comment": "this is version2",
              "aliases": [
                "alias2"
              ],
              "uri": "/tmp/version2",
              "properties": {},
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-11T06:56:06.340031Z"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_model_operation().load_model_version(
            catalog_name, schema_name, model_name, version
        )

    @mcp.tool(tags={"model"})
    async def load_model_version_by_alias(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        model_name: str,
        alias: str,
    ):
        """
        Load detailed information of a specific version of a model by its alias.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter models by.
            schema_name (str): The name of the schema to filter models by.
            model_name (str): The name of the model to load.
            alias (str): Alias of the model version.

        Returns:
            str: A JSON string containing full model version metadata.

        Example Return Value:
            {
              "version": 1,
              "comment": "this is version2",
              "aliases": [
                "alias2"
              ],
              "uri": "/tmp/version2",
              "properties": {},
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-11T06:56:06.340031Z"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_model_operation().load_model_version_by_alias(
            catalog_name, schema_name, model_name, alias
        )
