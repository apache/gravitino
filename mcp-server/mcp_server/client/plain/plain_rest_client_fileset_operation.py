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
from mcp_server.client.plain.utils import (
    encode_path_segment,
    extract_content_from_response,
)


class PlainRESTClientFilesetOperation(FilesetOperation):
    def __init__(self, metalake_name: str, rest_client):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def list_of_filesets(
        self, catalog_name: str, schema_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/filesets"
        )
        return extract_content_from_response(response, "identifiers", [])

    async def load_fileset(
        self, catalog_name: str, schema_name: str, fileset_name: str
    ) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/filesets/{encode_path_segment(fileset_name)}"
        )
        return extract_content_from_response(response, "fileset", {})

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
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/filesets/{encode_path_segment(fileset_name)}/files",
            params={"sub_path": sub_path, "location_name": location_name},
        )
        return extract_content_from_response(response, "files", [])

    async def create_fileset(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        fileset_type: str,
        storage_location: str,
        comment: str,
        properties: dict,
    ) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}/filesets",
            json={
                "name": name,
                "type": fileset_type,
                "storageLocation": storage_location,
                "comment": comment,
                "properties": properties,
            },
        )
        return extract_content_from_response(response, "fileset", {})

    async def alter_fileset(
        self,
        catalog_name: str,
        schema_name: str,
        fileset_name: str,
        updates: list,
    ) -> str:
        response = await self.rest_client.put(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/filesets/{encode_path_segment(fileset_name)}",
            json={"updates": updates},
        )
        return extract_content_from_response(response, "fileset", {})

    async def drop_fileset(
        self, catalog_name: str, schema_name: str, fileset_name: str
    ) -> str:
        response = await self.rest_client.delete(
            f"/api/metalakes/{encode_path_segment(self.metalake_name)}"
            f"/catalogs/{encode_path_segment(catalog_name)}"
            f"/schemas/{encode_path_segment(schema_name)}"
            f"/filesets/{encode_path_segment(fileset_name)}"
        )
        return extract_content_from_response(response, "dropped", False)
