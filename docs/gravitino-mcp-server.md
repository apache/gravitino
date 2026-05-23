---
title: "MCP Server"
slug: "/gravitino-mcp-server"
keyword: "Gravitino MCP metadata"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Gravitino MCP server lets LLM tools manage Gravitino metadata through the Model Context Protocol.

## Requirements

1. Python 3.10 or later.
2. `uv` installed. Follow the [official guide](https://docs.astral.sh/uv/getting-started/installation/).

## Usage

1. Clone the repository from GitHub and `cd` into the `mcp-server` directory.
2. Create a virtual environment: `uv venv`.
3. Install the required Python packages: `uv pip install -e .`.
4. Register the Gravitino MCP server with your LLM tool. For example, with Cursor, edit `~/.cursor/mcp.json` and add this configuration for a local server:

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
        "--gravitino-uri",
        "http://127.0.0.1:8090"
      ]
    }
  }
}
```

To run the MCP server over HTTP instead, start it with:

```shell
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 --transport http --mcp-url http://localhost:8000/mcp
```

Then use this configuration:

```json
{
  "mcpServers": {
    "gravitino": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

## Docker

Run the MCP server from the Docker image:

```shell
docker run -p 8000:8000 --network=host apache/gravitino-mcp-server:latest \
  --metalake test \
  --transport http \
  --mcp-url http://0.0.0.0:8000/mcp \
  --gravitino-uri http://127.0.0.1:8090
```

The MCP server inside a Docker container does not support the `stdio` transport.

## Supported Tools

The Gravitino MCP server supports the following tools. You can export tools by tag.

| Tool name                           | Description                                                                    | Tag          | Since version |
|-------------------------------------|--------------------------------------------------------------------------------|--------------|---------------|
| `get_list_of_catalogs`              | Retrieve a list of all catalogs in the system.                                 | `catalog`    | 1.0.0         |
| `get_list_of_schemas`               | Retrieve a list of schemas belonging to a specific catalog.                    | `schema`     | 1.0.0         |
| `get_list_of_tables`                | Retrieve a list of tables within a specific catalog and schema.                | `table`      | 1.0.0         |
| `get_table_metadata_details`        | Retrieve comprehensive metadata details for a specific table.                  | `table`      | 1.0.0         |
| `list_of_models`                    | Retrieve a list of models within a specific catalog and schema.                | `model`      | 1.0.0         |
| `load_model`                        | Retrieve comprehensive metadata details for a specific model.                  | `model`      | 1.0.0         | 
| `list_model_versions`               | Retrieve a list of versions for a specific model.                              | `model`      | 1.0.0         |
| `load_model_version`                | Retrieve comprehensive metadata details for a specific model version.          | `model`      | 1.0.0         | 
| `load_model_version_by_alias`       | Retrieve comprehensive metadata details for a specific model version by alias. | `model`      | 1.0.0         |
| `metadata_type_to_fullname_formats` | Retrieve the metadata type to fullname formats mapping.                        | `metadata`   | 1.0.0         |
| `list_of_topics`                    | Retrieve a list of topics within a specific catalog and schema.                | `topic`      | 1.0.0         |
| `load_topic`                        | Retrieve comprehensive metadata details for a specific topic.                  | `topic`      | 1.0.0         |
| `list_of_filesets`                  | Retrieve a list of filesets within a specific catalog and schema.              | `fileset`    | 1.0.0         |
| `load_fileset`                      | Retrieve comprehensive metadata details for a specific fileset.                | `fileset`    | 1.0.0         |
| `list_files_in_fileset`             | Retrieve a list of files within a specific fileset.                            | `fileset`    | 1.0.0         |
| `list_of_jobs`                      | Retrieve a list of jobs                                                        | `job`        | 1.0.0         |
| `get_job_by_id`                     | Retrieve a job by its ID.                                                      | `job`        | 1.0.0         |
| `list_of_job_templates`             | Retrieve a list of job templates.                                              | `job`        | 1.0.0         |
| `get_job_template_by_name`          | Retrieve a job template by its name.                                           | `job`        | 1.0.0         |
| `run_job`                           | Run a job with the specified parameters.                                       | `job`        | 1.0.0         |
| `cancel_job`                        | Cancel a running job by its ID.                                                | `job`        | 1.0.0         |
| `get_tag_by_name`                   | Retrieve a tag by its name.                                                    | `tag`        | 1.0.0         |
| `list_of_tags`                      | Retrieve a list of tags.                                                       | `tag`        | 1.0.0         |
| `list_tags_for_metadata`            | Retrieve a list of tags associated with a specific metadata item.              | `tag`        | 1.0.0         |
| `list_metadata_by_tag`              | Retrieve a list of metadata items associated with a specific tag.              | `tag`        | 1.0.0         |
| `associate_tag_with_metadata`       | Associate tags with a specific metadata item.                                  | `tag`        | 1.0.0         |
| `disassociate_tag_from_metadata`    | Disassociate tags from a specific metadata item.                               | `tag`        | 1.0.0         |
| `list_statistics_for_metadata`      | Retrieve a list of statistics associated with a specific metadata item.        | `statistics` | 1.0.0         |
| `list_statistics_for_partition`     | Retrieve a list of statistics associated with a specific partition.            | `statistics` | 1.0.0         |
| `get_list_of_policies`              | Retrieve a list of policies in the system.                                     | `policy`     | 1.0.0         |
| `get_policy_detail_information`     | Retrieve detailed information for a specific policy by policy name.            | `policy`     | 1.0.0         |
| `list_policies_for_metadata`        | List all policies associated with a specific metadata item.                    | `policy`     | 1.0.0         |
| `list_metadata_by_policy`           | List all metadata items associated with a specific policy.                     | `policy`     | 1.0.0         |
| `get_policy_for_metadata`           | Get a policy associated with a specific metadata item.                         | `policy`     | 1.0.0         |


## Configuration

Configure the Gravitino MCP server through command-line arguments. Run `uv run mcp_server -h` for the full list.

| Argument          | Description                                                     | Default value               | Required | Since version |
|-------------------|-----------------------------------------------------------------|-----------------------------|----------|---------------|
| `--metalake`      | The Gravitino metalake name.                                    | none                        | Yes      | 1.0.0         |
| `--gravitino-uri` | The URI of Gravitino server.                                    | `http://127.0.0.1:8090`     | No       | 1.0.0         |
| `--transport`     | Transport protocol type: stdio (local), http (Streamable HTTP). | `stdio`                     | No       | 1.0.0         |
| `--mcp-url`       | The url of MCP server if using http transport.                  | `http://127.0.0.1:8000/mcp` | No       | 1.0.0         |
