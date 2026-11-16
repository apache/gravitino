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

import json
from typing import Optional

from fastmcp import Context, FastMCP

_READONLY_TOOL_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
}


def _clean_optional_str(value) -> Optional[str]:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped if stripped else None


def load_topic_tools(mcp: FastMCP):
    @mcp.tool(tags={"topic"}, annotations=_READONLY_TOOL_ANNOTATIONS)
    async def list_of_topics(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
    ) -> str:
        """
        Retrieve a list of topics within a specific catalog and schema.
        This function returns a JSON-formatted string containing topic identifiers
        filtered by the specified catalog and schema names. Topics may include
        standard Kafka topics as well as AutoMQ Table Topics (configured via
        `automq.table.topic.*` and linked via `table-topic.*` properties);
        use `load_topic` or `get_topic_iceberg_linkage` to inspect linkage
        metadata.


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

    @mcp.tool(tags={"topic"}, annotations=_READONLY_TOOL_ANNOTATIONS)
    async def load_topic(
        ctx: Context,
        catalog_name: str,
        schema_name: str,
        topic_name: str,
    ) -> str:
        """
        Load detailed information of a specific topic.
        This function retrieves metadata for a topic identified by its name within the specified catalog and schema.
        When an AutoMQ Table Topic is configured
        (`automq.table.topic.enable=true`), returned properties also include
        synthetic Iceberg linkage metadata (`table-topic.*`).

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

    @mcp.tool(tags={"topic"}, annotations=_READONLY_TOOL_ANNOTATIONS)
    async def get_topic_iceberg_linkage(
        ctx: Context, catalog_name: str, schema_name: str, topic_name: str
    ) -> dict:
        """
        Resolve AutoMQ Table Topic configuration and linked Apache Iceberg
        table metadata for a topic.

        Args:
            ctx (Context): The request context object.
            catalog_name (str): Name of the Kafka catalog.
            schema_name (str): Name of the schema containing the topic.
            topic_name (str): Name of the topic to inspect.

        Returns:
            dict: Structured linkage metadata including whether Table Topic
            materialization is enabled and the target Iceberg catalog,
            namespace, table, and identifier.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        raw_topic = await client.as_topic_operation().load_topic(
            catalog_name, schema_name, topic_name
        )
        topic = (
            json.loads(raw_topic) if isinstance(raw_topic, str) else raw_topic
        )
        props = topic.get("properties") or {}
        resolved_topic_name = (
            _clean_optional_str(topic.get("name")) or topic_name
        )
        table_topic_enabled = (
            str(
                props.get(
                    "table-topic.enabled",
                    props.get("automq.table.topic.enable", "false"),
                )
            )
            .strip()
            .lower()
            == "true"
        )
        if table_topic_enabled:
            iceberg_catalog = _clean_optional_str(
                props.get("table-topic.iceberg-catalog")
            )
            iceberg_namespace = (
                _clean_optional_str(props.get("table-topic.iceberg-namespace"))
                or _clean_optional_str(
                    props.get("automq.table.topic.namespace")
                )
                or "default"
            )
            iceberg_table = (
                _clean_optional_str(props.get("table-topic.iceberg-table"))
                or resolved_topic_name
            )
            iceberg_ident = _clean_optional_str(
                props.get("table-topic.iceberg-ident")
            ) or (
                f"{ctx.request_context.lifespan_context.resolve_metalake()}."
                f"{iceberg_catalog}.{iceberg_namespace}.{iceberg_table}"
                if iceberg_catalog
                else f"{iceberg_namespace}.{iceberg_table}"
            )
        else:
            iceberg_catalog = None
            iceberg_namespace = None
            iceberg_table = None
            iceberg_ident = None

        return {
            "topic_name": resolved_topic_name,
            "table_topic_enabled": table_topic_enabled,
            "iceberg_catalog": iceberg_catalog,
            "iceberg_namespace": iceberg_namespace,
            "iceberg_table": iceberg_table,
            "iceberg_ident": iceberg_ident,
            "schema_type": _clean_optional_str(
                props.get("automq.table.topic.schema.type")
            ),
            "commit_interval_ms": _clean_optional_str(
                props.get("automq.table.topic.commit.interval.ms")
            ),
            "upsert_enabled": (
                str(props.get("automq.table.topic.upsert.enable", "false"))
                .strip()
                .lower()
                == "true"
                if table_topic_enabled
                else False
            ),
            "id_columns": _clean_optional_str(
                props.get("automq.table.topic.id.columns")
            ),
            "partition_by": _clean_optional_str(
                props.get("automq.table.topic.partition.by")
            ),
            "cdc_field": _clean_optional_str(
                props.get("automq.table.topic.cdc.field")
            ),
            "errors_tolerance": _clean_optional_str(
                props.get("automq.table.topic.errors.tolerance")
            ),
            "convert_key_type": _clean_optional_str(
                props.get("automq.table.topic.convert.key.type")
            ),
            "convert_value_type": _clean_optional_str(
                props.get("automq.table.topic.convert.value.type")
            ),
            "transform_value_type": _clean_optional_str(
                props.get("automq.table.topic.transform.value.type")
            ),
        }
