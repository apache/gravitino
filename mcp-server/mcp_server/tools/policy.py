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

    @mcp.tool(tags={"policy", "tag"})
    async def list_policies_for_tag(ctx: Context, tag_name: str) -> str:
        """List the policies directly associated with a tag.

        The result includes each policy and the selector on its association.
        ALL_VALUES selectors match any assignment of the tag, while TAG_VALUE
        selectors match only the specified assignment value.

        Args:
            ctx (Context): The request context containing the REST client.
            tag_name (str): Name of the tag.

        Returns:
            str: JSON-formatted policy-tag associations.

        Example Return Value:
            [
              {
                "policy": {
                  "name": "retention_policy",
                  "policyType": "custom",
                  "enabled": true
                },
                "selector": {
                  "type": "TAG_VALUE",
                  "value": "finance"
                }
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().list_policies_for_tag(
            tag_name
        )

    @mcp.tool(tags={"policy", "tag"})
    async def associate_policy_with_tag(
        ctx: Context,
        tag_name: str,
        policy_name: str,
        selector: dict,
    ) -> str:
        """Associate one policy with a tag and a selector.

        Use {"type": "ALL_VALUES"} to select the policy whenever the tag is
        present. Use {"type": "TAG_VALUE", "value": "finance"} to select it
        only when the tag is assigned with that value.

        Args:
            ctx (Context): The request context containing the REST client.
            tag_name (str): Name of the tag.
            policy_name (str): Name of the policy.
            selector (dict): Required association selector.

        Returns:
            str: JSON-formatted policy-tag association.

        Example Return Value:
            {
              "code": 0,
              "policy": "retention_policy",
              "tag": "data_domain",
              "selector": {
                "type": "TAG_VALUE",
                "value": "finance"
              }
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().associate_policy_with_tag(
            tag_name, policy_name, selector
        )

    @mcp.tool(tags={"policy", "tag"})
    async def disassociate_policy_from_tag(
        ctx: Context, tag_name: str, policy_name: str
    ) -> str:
        """Remove one direct policy association from a tag.

        Args:
            ctx (Context): The request context containing the REST client.
            tag_name (str): Name of the tag.
            policy_name (str): Name of the policy.

        Returns:
            str: JSON-formatted removal confirmation.

        Example Return Value:
            {
              "policy": "retention_policy",
              "tag": "data_domain",
              "removed": true
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().disassociate_policy_from_tag(
            tag_name, policy_name
        )

    @mcp.tool(tags={"policy", "tag"})
    async def list_tags_for_policy(ctx: Context, policy_name: str) -> str:
        """List the tags directly associated with a policy.

        The result includes each tag and the selector on its association.

        Args:
            ctx (Context): The request context containing the REST client.
            policy_name (str): Name of the policy.

        Returns:
            str: JSON-formatted policy-tag associations.

        Example Return Value:
            [
              {
                "tag": {
                  "name": "data_domain",
                  "comment": "Business data domain"
                },
                "selector": {
                  "type": "TAG_VALUE",
                  "value": "finance"
                }
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_policy_operation().list_tags_for_policy(
            policy_name
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
