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

import asyncio

from fastmcp import Client

client = Client("__main__.py")


async def call_tool(name: str):
    async with client:
        result = await client.call_tool("greet", {"name": name})
        print(result)


async def get_list_catalogs():
    async with client:
        result = await client.call_tool("get_list_of_catalogs", {})
        print(result)


async def get_list_schemas(catalog_name: str):
    async with client:
        result = await client.call_tool(
            "get_list_of_schemas", {"catalog_name": catalog_name}
        )
        print(result)


asyncio.run(get_list_catalogs())
