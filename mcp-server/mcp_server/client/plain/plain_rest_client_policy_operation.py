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

from mcp_server.client import PolicyOperation


class PlainRESTClientPolicyOperation(PolicyOperation):
    """
    Implementation of PolicyOperation using a plain REST client.
    """

    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def get_list_of_policies(self) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/policies"
        )
        return response.json().get("names", [])

    async def load_policy(
        self,
        policy_name: str,
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/policies/{policy_name}"
        )
        return response.json().get("policy", {})
