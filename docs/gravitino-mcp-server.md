---
title: "MCP Server"
slug: "/gravitino-mcp-server"
keyword: "Gravitino MCP metadata"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino MCP server provides the ability to manage Gravitino metadata for LLM.

## Requirements

1. Python 3.10+
2. uv is installed. Install uv by following the [official guide](https://docs.astral.sh/uv/getting-started/installation/).

## Usage

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
        "--gravitino-uri",
        "http://127.0.0.1:8090"
      ]
    }
  }
}
```

Or start an HTTP MCP server by `uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 --transport http --mcp-url http://localhost:8000/mcp`, and use the configuration:

```json
{
  "mcpServers": {
    "gravitino": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

## Docker Instructions

You could start Gravitino MCP server by Docker image, `docker run -p 8000:8000 --network=host apache/gravitino-mcp-server:latest --metalake test --transport http --mcp-url http://0.0.0.0:8000/mcp --gravitino-uri http://127.0.0.1:8090`. Please note that the MCP server in Docker container doesn't support `stdio` transport mode.

## Supported Tools

Gravitino MCP server supports the following tools, and you could export tool by tag.

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

You could config Gravitino MCP server by arguments, `uv run mcp_server -h` shows the detailed information.

| Argument          | Description                                                                      | Default value               | Required | Since version |
|-------------------|----------------------------------------------------------------------------------|-----------------------------|----------|---------------|
| `--metalake`      | The Gravitino metalake name.                                                     | none                        | Yes      | 1.0.0         |
| `--gravitino-uri` | The URI of Gravitino server.                                                     | `http://127.0.0.1:8090`     | No       | 1.0.0         |
| `--transport`     | Transport protocol: stdio (local), http / streamable-http (Streamable HTTP).     | `stdio`                     | No       | 1.0.0         |
| `--mcp-url`       | The URL of MCP server if using HTTP transport.                                   | `http://127.0.0.1:8000/mcp` | No       | 1.0.0         |
| `--token`         | OAuth2 Bearer token for Gravitino; or set `GRAVITINO_TOKEN`. See Authentication. | none (anonymous)            | No       | 1.3.0         |
| `--tls-cert`      | PEM certificate to serve the endpoint over HTTPS. Requires `--tls-key`.          | none                        | No       | 1.3.0         |
| `--tls-key`       | PEM private key to serve the endpoint over HTTPS. Requires `--tls-cert`.         | none                        | No       | 1.3.0         |

## Authentication

By default the MCP server talks to Gravitino anonymously. There are two ways to attach an identity, depending on the transport.

### Static startup token (stdio and HTTP)

Pass `--token` (or set the `GRAVITINO_TOKEN` environment variable) to authenticate the server with a static OAuth2 Bearer token. The value is treated as a Bearer token and sent as `Authorization: Bearer <token>`. The token is masked in the server's log output.

```shell
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 --token <your-token>
# or
export GRAVITINO_TOKEN=<your-token>
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090
```

In `stdio` mode this token is used for every request. In HTTP mode it is only the fallback, used when an incoming request does not carry its own `Authorization` header.

### Per-request identity (HTTP)

When the server runs with HTTP transport, the `Authorization` header of each incoming MCP request is forwarded verbatim to Gravitino. The scheme is preserved, so OAuth2 (`Bearer`), Gravitino simple authentication (`Basic <base64(user:dummy)>`) and others all work. This keeps concurrent sessions from different principals isolated — one principal's identity never leaks into another's calls — and lets Gravitino enforce authorization per caller. The per-request header takes priority over the static `--token`.

Authorization itself is always enforced by Gravitino: the MCP server forwards the identity but does not make access-control decisions of its own.

### Serving over HTTPS (TLS)

To serve the MCP HTTP endpoint (the `--mcp-url`, not the `--gravitino-uri`) over TLS, provide both `--tls-cert` and `--tls-key` and use an `https://` `--mcp-url`. The certificate and key must be provided together, and the URL scheme must match the TLS setting (an `https://` URL without a cert/key, or a cert/key behind an `http://` URL, is rejected at startup).

```shell
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 \
  --transport streamable-http --mcp-url https://localhost:8000/mcp \
  --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```

## Audit Logging

Every tool invocation is recorded as one structured JSON line in `gravitino-mcp-audit.log` (written to the server's working directory). Each record is attributed to the principal derived from the request's `Authorization` header.

| Field        | Description                                                                                                                                                                             |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `timestamp`  | UTC ISO-8601 time of the call.                                                                                                                                                          |
| `principal`  | Caller identity: username for `Basic` simple auth, `bearer:<first-8-chars>` for a Bearer token, or `anonymous` when no identity is present.                                             |
| `tool`       | Name of the invoked MCP tool.                                                                                                                                                           |
| `outcome`    | `allow` for successful calls, `deny` for failed ones. `deny` is emitted for any tool-call exception (authorization denial being the common case); inspect `error_type` to disambiguate. |
| `error_type` | Exception class name, present only when `outcome` is `deny`.                                                                                                                            |

Example record:

```json
{"timestamp": "2026-06-16T03:21:09.123456+00:00", "principal": "alice", "tool": "get_list_of_catalogs", "outcome": "allow"}
```
