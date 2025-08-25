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


def load_tag_tool(mcp: FastMCP):
    @mcp.tool(tags={"tag"}, enabled=False)
    async def create_tag(
        ctx: Context, name: str, comment: str, properties: dict
    ) -> str:
        """
        Create a new tag within the specified metalake.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            name (str): Name of the tag to be created
            comment (str): Description or comment for the tag
            properties (dict): Dictionary of key-value pairs representing tag properties

        Returns:
            str: JSON-formatted string containing the created tag information

        Example Return Value:
            {
              "name": "tag1",
              "comment": "This is a tag",
              "properties": {
                "key1": "value1",
                "key2": "value2"
              },
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-07T06:20:18.330570Z"
              },
              "inherited": null
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().create_tag(
            name, comment, properties
        )

    @mcp.tool(tags={"tag"})
    async def get_tag_by_name(ctx: Context, tag_name: str) -> str:
        """
        Load a tag by its name.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            tag_name (str): Name of the tag to get

        Returns:
            str: JSON-formatted string containing the tag information

        Example Return Value:
            {
              "name": "tag1",
              "comment": "This is a tag",
              "properties": {
                "key1": "value1",
                "key2": "value2"
              },
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-07T06:20:18.330570Z"
              },
              "inherited": null
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().get_tag_by_name(tag_name)

    @mcp.tool(tags={"tag"})
    async def list_of_tags(ctx: Context) -> str:
        """
        Retrieve the list of tags within the metalake.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.

        Returns:
            str: JSON-formatted string containing tag list information

        Example Return Value:
            [
              {
                "name": "tag1",
                "comment": "This is a tag",
                "properties": {
                  "key1": "value1",
                  "key2": "value2"
                },
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-08-07T06:20:18.330570Z"
                },
                "inherited": null
              },
              ...
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().list_of_tags()

    # Disable the alter_tag tool by default as it can be destructive.
    @mcp.tool(tags={"tag"}, enabled=False)
    async def alter_tag(ctx: Context, name: str, updates: list) -> str:
        """
        Alter an existing tag within the specified metalake.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            name (str): Name of the tag to be altered
            updates (list): List of update operations to be applied to the tag

        Example input for updates:
            [
                {
                  "@type": "rename",
                  "newName": "tag2"
                },
                {
                  "@type": "updateComment",
                  "newComment": "This is an updated tag"
                },
                {
                  "@type": "setProperty",
                  "property": "key3",
                  "value": "value3"
                },
                {
                  "@type": "removeProperty",
                  "property": "key1"
                }
            ]

        Returns:
            str: JSON-formatted string containing the altered tag information

        Example Return Value:
            {
              "name": "tag1",
              "comment": "Updated comment",
              "properties": {
                "key1": "new_value1"
              },
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-07T06:20:18.330570Z"
              },
              "inherited": null
            }
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().alter_tag(name, updates)

    # Disable the delete_tag tool by default as it can be destructive.
    @mcp.tool(tags={"tag"}, enabled=False)
    async def delete_tag(ctx: Context, name: str) -> None:
        """
        Delete a tag by its name.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            name (str): Name of the tag to delete

        Returns:
            None

        Raises:
            Exception: If the deletion fails, an exception is raised with an error message.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().delete_tag(name)

    @mcp.tool(tags={"tag"})
    async def associate_tag_with_metadata(
        ctx: Context,
        metadata_full_name: str,
        metadata_type: str,
        tags_to_associate: list,
    ) -> str:
        """
        Associate tags with metadata.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata object to associate tags with.
            It's typically in the format "catalog.schema.table" or "catalog.schema" or "catalog".
            or "catalog.schema.fileset". The "model", "topic" are also supported and the format
            is the same as for "catalog.schema.table". For more, please see tool
             `get_metadata_fullname_formats`.

            metadata_type (str): Type of the metadata object (e.g., "table", "schema", "catalog",
             "fileset", "model", "topic"). For More information, please see
             tool `list_all_metadata_types`.
            tags_to_associate (list): List of tag names to associate with the metadata.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"
            tags_to_associate: ["tag1", "tag2"]
            tags_to_disassociate: ["tag3"]

        Returns:
            str: JSON-formatted string containing the names of associated tags.

        Example Return Value:
            [
              "tag1",
              "tag2"
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().associate_tag_with_metadata(
            metadata_full_name,
            metadata_type,
            tags_to_associate,
            [],
        )

    @mcp.tool(tags={"tag"})
    async def disassociate_tag_from_metadata(
        ctx: Context,
        metadata_full_name: str,
        metadata_type: str,
        tags_to_disassociate: list,
    ) -> str:
        """
        Disassociate tags from metadata.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata object to disassociate tags from.
            For more, please see tool `get_metadata_fullname_formats`.
            metadata_type (str): Type of the metadata object (e.g., "table", "schema", "catalog",
             "fileset", "model", "topic"). For More information, please see
             tool `list_all_metadata_types`.
            tags_to_disassociate (list): List of tag names to disassociate from the metadata.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"
            tags_to_disassociate: ["tag1", "tag2"]

        Returns:
            str: JSON-formatted string containing the names of disassociated tags.

        Example Return Value:
            [
              "tag1",
              "tag2"
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().associate_tag_with_metadata(
            metadata_full_name, metadata_type, [], tags_to_disassociate
        )

    @mcp.tool(tags={"tag"})
    async def list_tags_for_metadata(
        ctx: Context, metadata_full_name: str, metadata_type: str
    ) -> str:
        """
        List all tags associated with a specific metadata item.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            metadata_full_name (str): Full name of the metadata item (e.g., table, column). For more, please see tool
             `get_metadata_fullname_formats`.
            metadata_type (str): Type of the metadata (e.g., "table", "column"). For More information, please see
             tool `list_all_metadata_types`.

        Example input:
            metadata_full_name: "catalog.schema.table"
            metadata_type: "table"

        Returns:
            str: JSON-formatted string containing the list of tags associated with the metadata.

        Example Return Value:
            [
              {
                "name": "import",
                "comment": "Import tag for data governance",
                "properties": {},
                "audit": {
                  "creator": "anonymous",
                  "createTime": "2025-08-12T11:41:09.257235Z"
                },
                "inherited": false
              }
            ]
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_tag_operation().list_tags_for_metadata(
            metadata_full_name, metadata_type
        )

    @mcp.tool(tags={"tag"})
    async def list_metadata_by_tag(ctx: Context, tag_name: str) -> str:
        """
        List all metadata items associated with a specific tag.

        Args:
            ctx (Context): The request context object containing lifespan context
                           and connector information.
            tag_name (str): Name of the tag to filter metadata by.

        Returns:
            str: JSON-formatted string containing the list of metadata items associated with the tag.

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
        return await client.as_tag_operation().list_metadata_by_tag(tag_name)
