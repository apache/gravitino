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


def load_topic_tools(mcp: FastMCP):
    @mcp.tool(tags={"topic"})
    async def list_of_topics(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ):
        """
        Retrieve a list of topics within a specific catalog and schema.
        This function returns a JSON-formatted string containing topic identifiers
        filtered by the specified catalog and schema names.


        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog to filter topics by.
            schema_name (str): The name of the schema to filter topics by.
        Returns:
            str: A JSON string representing an array of topic objects with the following structure:
            - namespace: An array of strings representing the hierarchical namespace of the topic.
            - name: The name of the topic.

            [
              {
                "namespace": [
                  "test",
                  "kafka_catalog",
                  "default"
                ],
                "name": "topic1"
              },
              {
                "namespace": [
                  "test",
                  "kafka_catalog",
                  "default"
                ],
                "name": "topic2"
              }
            ]
        Example Return Value:
            [
              {
                "namespace": [
                  "test",
                  "kafka_catalog",
                  "default"
                ],
                "name": "topic1"
              },
              {
                "namespace": [
                  "test",
                  "kafka_catalog",
                  "default"
                ],
                "name": "topic2"
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_topic_operation().list_of_topics(
            catalog_name, schema_name
        )

    @mcp.tool(tags={"topic"})
    async def load_topic(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        topic_name: str,
    ):
        """
        Load detailed information of a specific topic.
        This function retrieves metadata for a topic identified by its name within the specified catalog and schema.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            catalog_name (str): The name of the catalog containing the topic.
            schema_name (str): The name of the schema containing the topic.
            topic_name (str): The name of the topic to load.

        Returns:
            str: A JSON string containing full topic metadata.

        Example Return Value:
            {
              "name": "test-topic_925a2038",
              "properties": {
                "compression.type": "producer",
                "leader.replication.throttled.replicas": "",
                "remote.storage.enable": "false",
                "message.downconversion.enable": "true",
                "min.insync.replicas": "1",
                "segment.jitter.ms": "0",
                "local.retention.ms": "-2",
                "cleanup.policy": "delete",
                "flush.ms": "9223372036854775807",
                "follower.replication.throttled.replicas": "",
                "segment.bytes": "1073741824",
                "preallocate": "false"
              },
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-11T03:31:53.100944Z"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_topic_operation().load_topic(
            catalog_name, schema_name, topic_name
        )

    @mcp.tool(tags={"topic"})
    # pylint: disable=too-many-positional-arguments
    async def create_topic(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        name: str,
        comment: str,
        properties: dict,
    ) -> str:
        """
        Create a new topic within a schema.

        Authorization is enforced by Gravitino: a principal without the
        required grant receives an authorization denial.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
            name (str): Name of the topic to create.
            comment (str): Human-readable description.
            properties (dict): Topic configuration properties.

        Returns:
            str: JSON-formatted string containing the created topic.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_topic_operation().create_topic(
            catalog_name, schema_name, name, comment, properties
        )

    @mcp.tool(tags={"topic"})
    async def alter_topic(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        topic_name: str,
        updates: list,
    ) -> str:
        """
        Alter an existing topic.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
            topic_name (str): Name of the topic to alter.
            updates (list): List of update operations. Example:
                [
                  {"@type": "updateComment", "newComment": "updated"},
                  {"@type": "setProperty", "property": "k", "value": "v"}
                ]

        Returns:
            str: JSON-formatted string containing the altered topic.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_topic_operation().alter_topic(
            catalog_name, schema_name, topic_name, updates
        )

    @mcp.tool(tags={"topic"})
    async def delete_topic(
        ctx: Context, catalog_name: str, schema_name: str, topic_name: str
    ) -> str:
        """
        Delete a topic by its name.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the catalog.
            schema_name (str): Name of the schema.
            topic_name (str): Name of the topic to delete.

        Returns:
            str: JSON-formatted string indicating whether the topic was deleted.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_topic_operation().delete_topic(
            catalog_name, schema_name, topic_name
        )
