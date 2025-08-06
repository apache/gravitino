<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

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

Or start HTTP MCP server by `uv run mcp_server --metalake test --uri http://127.0.0.1:8090 --transport http --mcp-url http://localhost:8000/mcp`, and use the configuration:

```json
{
  "mcpServers": {
    "gravitino": {
      "url": "http://localhost:1234/mcp1"
    }
  }
}
```
