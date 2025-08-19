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
        Retrieve the list of available policy names.

        Parameters:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
        Returns:
            str: A JSON string representing an array of policy names, each name
            represents a distinct policy configured in the system.

        Example Response:
        [
            "network_https_policy",
            "data_retention_7yrs",
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
                policyType: Classification of policy (e.g., "custom", "system") (string)
                enabled: Activation status (boolean)
                content: Rule definitions and scope settings (dict)
                  - customRules: Key-value pairs of rule identifiers and settings (dict)
                  - properties: Additional configuration properties (dict)
                  - supportedObjectTypes: Entities this policy applies to (list of string)
                inherited: Parent policy reference if applicable (null or dict)
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