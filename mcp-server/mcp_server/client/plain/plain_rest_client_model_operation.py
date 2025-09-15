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

from mcp_server.client import ModelOperation


class PlainRESTClientModelOperation(ModelOperation):
    """
    Implementation of ModelOperation using a plain REST client.
    """

    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_models(self, catalog_name: str, schema_name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models"
        )
        return response.json().get("identifiers", [])

    async def load_model(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}"
        )
        return response.json().get("model", {})

    async def list_model_versions(
        self, catalog_name: str, schema_name: str, model_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}"
            f"/versions?details=true"
        )
        return response.json().get("infos", [])

    async def load_model_version(
        self, catalog_name: str, schema_name: str, model_name: str, version: int
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}"
            f"/versions/{version}"
        )
        return response.json().get("modelVersion", {})

    async def load_model_version_by_alias(
        self, catalog_name: str, schema_name: str, model_name: str, alias: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/models/{model_name}/"
            f"aliases/{alias}"
        )
        return response.json().get("modelVersion", {})
