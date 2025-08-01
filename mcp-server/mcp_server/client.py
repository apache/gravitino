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
