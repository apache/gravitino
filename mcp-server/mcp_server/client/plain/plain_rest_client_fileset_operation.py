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

from mcp_server.client.fileset_operation import FilesetOperation


class PlainRESTClientFilesetOperation(FilesetOperation):
    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_filesets(
        self, catalog_name: str, schema_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/filesets"
        )
        return response.json().get("identifiers", [])

    async def load_fileset(
        self, catalog_name: str, schema_name: str, fileset_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}/filesets/{fileset_name}"
        )
        return response.json().get("fileset", {})

    # pylint: disable=too-many-positional-arguments
    async def list_files_in_fileset(
        self,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
        location_name: str,
        sub_path: str = "/",
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/catalogs/{catalog_name}/schemas/{schema_name}"
            f"/filesets/{fileset_name}/files?sub_path={sub_path}&location_name={location_name}"
        )
        return response.json().get("files", [])
