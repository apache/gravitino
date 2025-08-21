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


def load_policy_tools(mcp: FastMCP):
    @mcp.tool(tags={"policy"})
    async def get_list_of_policies(
        ctx: Context,
    ) -> str:
        """
        Retrieve the list of available policy information.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
        Returns:
            str: A JSON string representing an array of policy information objects, each object
            contains the policy name, comment, policy type, enabled status, content, supported object types,
            inherited policy, and audit information.

        Example Response:
        [
            {
                "name": "my_policy1",
                "comment": "This is a test policy",
                "policyType": "custom",
                "enabled": true,
                "content": {
                    "customRules": {
                        "rule1": 123
                    },
                    "properties": {
                        "key1": "value1"
                    },
                    "supportedObjectTypes": [
                        "fileset",
                        "schema",
                        "topic",
                        "table",
                        "model",
                        "catalog"
                    ]
                },
                "inherited": null,
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2025-08-18T08:29:30.016501Z"
                }
            }
        ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().get_list_of_policies()

    @mcp.tool(tags={"policy"})
    async def get_policy_detail_information(
        ctx: Context,
        policy_name: str,
    ) -> str:
        """
        Retrieve detailed information for a specific policy by policy name.

        Parameters:
            ctx : Context
                The request context object containing lifespan context and connector
                information.
            policy_name : str
                The unique identifier for the policy. Must be one of the names
                returned by `get_list_of_policies`.

        Returns:
            str : A structured JSON object containing the policy configuration with the following fields:
                name: Unique policy identifier (string)
                comment: Descriptive text about the policy (string)
                policyType: Classification of policy (e.g., "custom") (string)
                enabled: Activation status (boolean)
                content: Rule definitions and scope settings (dict)
                  - customRules: Key-value pairs of rule identifiers and settings (dict)
                  - properties: Additional configuration properties (dict)
                  - supportedObjectTypes: Entities this policy applies to (list of string)
                inherited: Whether the policy is inherited from parent metadata (boolean)
                audit: Creation metadata (dict)
                  - creator: User or service that created the policy (string)
                  - createTime: ISO 8601 timestamp of creation (string)

        Example Response:
        {
            "name": "my_policy1",
            "comment": "This is a test policy",
            "policyType": "custom",
            "enabled": true,
            "content": {
                "customRules": {
                    "rule1": 123
                },
                "properties": {
                    "key1": "value1"
                },
                "supportedObjectTypes": [
                    "fileset",
                    "schema",
                    "topic",
                    "table",
                    "model",
                    "catalog"
                ]
            },
            "inherited": null,
            "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-18T08:29:30.016501Z"
            }
        }
        """

        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().load_policy(policy_name)

    @mcp.tool(tags={"policy"})
    async def associate_policy_with_metadata(
        ctx: Context,
        metadata_full_name: str,
        metadata_type: str,
        policies_to_add: list,
    ) -> str:
        """
        Associate policies with metadata.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata object to associate policies with.
            It's typically in the format "catalog.schema.table" or "catalog.schema" or "catalog"
            or "catalog.schema.fileset". The "model", "topic" are also supported and the format
            is the same as for "catalog.schema.table". For more, please see tool
             `get_metadata_fullname_formats`.

            metadata_type (str): Type of the metadata object (e.g., "table", "schema", "catalog",
             "fileset", "model", "topic"). For More information, please see
             tool `list_all_metadata_types`.
            policies_to_add (list): List of policy names to associate with the metadata.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"
            policies_to_add: ["policy1", "policy2"]

        Returns:
            str: JSON-formatted string containing the names of associated policies.

        Example Return Value:
            [
              "policy1",
              "policy2"
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return (
            await client.as_policy_operation().associate_policy_with_metadata(
                metadata_full_name,
                metadata_type,
                policies_to_add,
                [],
            )
        )

    @mcp.tool(tags={"policy"})
    async def disassociate_policy_from_metadata(
        ctx: Context,
        metadata_full_name: str,
        metadata_type: str,
        policies_to_remove: list,
    ) -> str:
        """
        Disassociate policies from metadata.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata object to disassociate policies from.
            For more, please see tool `get_metadata_fullname_formats`.
            metadata_type (str): Type of the metadata object (e.g., "table", "schema", "catalog",
             "fileset", "model", "topic"). For More information, please see
             tool `list_all_metadata_types`.
            policies_to_remove (list): List of policy names to disassociate from the metadata.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"
            policies_to_remove: ["policy1", "policy2"]

        Returns:
            str: JSON-formatted string containing the names of associated policies.

        Example Return Value:
            [
              "policy3"
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return (
            await client.as_policy_operation().associate_policy_with_metadata(
                metadata_full_name, metadata_type, [], policies_to_remove
            )
        )

    @mcp.tool(tags={"policy"})
    async def list_policies_for_metadata(
        ctx: Context, metadata_full_name: str, metadata_type: str
    ) -> str:
        """
        List all policies associated with a specific metadata item.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata item. For more, please see tool
             `get_metadata_fullname_formats`.
            metadata_type (str): Type of the metadata (e.g., "table", "column"). For More information, please see
             tool `list_all_metadata_types`.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"

        Returns:
            str: JSON-formatted string containing the list of policies associated with the metadata.

        Example Return Value:
            [
                {
                    "name": "my_policy1",
                    "comment": "This is a test policy",
                    "policyType": "custom",
                    "enabled": true,
                    "content": {
                        "customRules": {
                            "rule1": 123
                        },
                        "properties": {
                            "key1": "value1"
                        },
                        "supportedObjectTypes": [
                            "fileset",
                            "model",
                            "topic",
                            "schema",
                            "table",
                            "catalog"
                        ]
                    },
                    "inherited": false,
                    "audit": {
                        "creator": "anonymous",
                        "createTime": "2025-08-18T08:29:30.016501Z"
                    }
                }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().list_policies_for_metadata(
            metadata_full_name, metadata_type
        )

    @mcp.tool(tags={"policy"})
    async def list_metadata_by_policy(ctx: Context, policy_name: str) -> str:
        """
        List all metadata items associated with a specific policy.

        The child metadata items which inherit the associate from the parent metadata
        will be excluded in the result. For example, if the policy is associated with
        the catalog, the catalog's child metadata items will be excluded in the result.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            policy_name (str): Name of the policy to filter metadata by.

        Returns:
            str: JSON-formatted string containing the list of metadata items associated with the policy.

        Example Return Value:
            [
              {
                "fullName": "model_catalog",
                "type": "catalog"
              },
              {
                "fullName": "catalog.schema.table",
                "type": "table"
              },
              {
                "fullName": "catalog.schema.fileset",
                "type": "fileset"
              },
              {
                "fullName": "catalog.schema.model",
                "type": "model"
              },
              {
                "fullName": "catalog.schema.topic",
                "type": "topic"
              }
              {
                "fullName": "catalog.schema",
                "type": "schema"
              }
              ...
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().list_metadata_by_policy(
            policy_name
        )

    @mcp.tool(tags={"policy"})
    async def get_policy_for_metadata(
        ctx: Context,
        metadata_full_name: str,
        metadata_type: str,
        policy_name: str,
    ) -> str:
        """
        Get the policy associated with a specific metadata item by policy name.

        This tool is used to check whether a policy is associated with the metadata item.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata item. For more, please see tool
             `get_metadata_fullname_formats`.
            metadata_type (str): Type of the metadata (e.g., "table", "column"). For More information, please see
             tool `list_all_metadata_types`.
            policy_name (str): Name of the policy to filter metadata by.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"
            policy_name: "policy_a"

        Returns:
            str: JSON-formatted string containing the detailed policy information associated with the metadata.

        Example Return Value:
            {
                "name": "my_policy1",
                "comment": "This is a test policy",
                "policyType": "custom",
                "enabled": true,
                "content": {
                    "customRules": {
                        "rule1": 123
                    },
                    "properties": {
                        "key1": "value1"
                    },
                    "supportedObjectTypes": [
                        "fileset",
                        "schema",
                        "topic",
                        "table",
                        "model",
                        "catalog"
                    ]
                },
                "inherited": false,
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2025-08-18T08:29:30.016501Z"
                }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().get_policy_for_metadata(
            metadata_full_name, metadata_type, policy_name
        )
