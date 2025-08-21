---
title: "Gravitino MCP server"
slug: /gravitino-mcp-server
keyword: Gravitino MCP metadata
license: "This software is licensed under the Apache License version 2."
---

Gravitino MCP server provides the ability to manage Gravitino metadata for LLM.

### Requirement:

1. Python 3.10+
2. uv is installed. You can install uv by following the [official guide](https://docs.astral.sh/uv/getting-started/installation/).

### How to run:

1. Clone the code from GitHub, and change to `mcp-server` directory
2. Create virtual environment, `uv venv`
3. Install the required Python packages. `uv pip install -e .`
4. Add Gravitino MCP server to corresponding LLM tools. Take `cursor` for example, edit `~/.cursor/mcp.json`, use following configuration for local Gravitino MCP server:

```json
{
  "mcpServers": {
    "gravitino": {
      "command": "uv",
      "args": [
        "--directory",
        "$path/mcp-server",
        "run",
        "mcp_server",
        "--metalake",
        "test",
        "--uri",
        "http://127.0.0.1:8090"
      ]
    }
  }
}
```

Or start a HTTP MCP server by `uv run mcp_server --metalake test --uri http://127.0.0.1:8090 --transport http --mcp-url http://localhost:8000/mcp`, and use the configuration:

```json
{
  "mcpServers": {
    "gravitino": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

### Supported tools

Gravitino MCP server supports the following tools, and you could export tool by tag.

| Tool name                       | Description                                                         | Tag       | Since version |
|---------------------------------|---------------------------------------------------------------------|-----------|---------------|
| `get_list_of_catalogs`          | Retrieve a list of all catalogs in the system.                      | `catalog` | 1.0.0         |
| `get_list_of_schemas`           | Retrieve a list of schemas belonging to a specific catalog.         | `schema`  | 1.0.0         |
| `get_list_of_tables`            | Retrieve a list of tables within a specific catalog and schema.     | `table`   | 1.0.0         |
| `get_table_metadata_details`    | Retrieve comprehensive metadata details for a specific table.       | `table`   | 1.0.0         |
| `get_list_of_policies`          | Retrieve a list of policies in the system.                          | `policy`  | 1.0.0         |
| `get_policy_detail_information` | Retrieve detailed information for a specific policy by policy name. | `policy`  | 1.0.0         |
| `list_policies_for_metadata`    | List all policies associated with a specific metadata item.         | `policy`  | 1.0.0         |
| `list_metadata_by_policy`       | List all metadata items associated with a specific policy.          | `policy`  | 1.0.0         |
| `get_policy_for_metadata`       | Get a policy associated with a specific metadata item.              | `policy`  | 1.0.0         |

### Configuration

You could config Gravitino MCP server by arguments, `uv run mcp_server -h` shows the detailed information.

| Argument      | Description                                                     | Default value               | Required | Since version |
|---------------|-----------------------------------------------------------------|-----------------------------|----------|---------------|
| `--metalake`  | The Gravitino metalake name.                                    | none                        | Yes      | 1.0.0         |
| `--uri`       | The URI of Gravitino server.                                    | `http://127.0.0.1:8090`     | No       | 1.0.0         |
| `--transport` | Transport protocol type: stdio (local), http (Streamable HTTP). | `stdio`                     | No       | 1.0.0         |
| `--mcp-url`   | The url of MCP server if using http transport.                  | `http://127.0.0.1:8000/mcp` | No       | 1.0.0         |
