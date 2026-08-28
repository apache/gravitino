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
4. Add Gravitino MCP server to corresponding LLM tools. Take Cursor for example, edit `~/.cursor/mcp.json`, use following configuration for local Gravitino MCP server:

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
      ],
      "env": {
        "GRAVITINO_OAUTH_TOKEN_ENDPOINT": "https://idp.example/realms/gravitino/protocol/openid-connect/token",
        "GRAVITINO_OAUTH_CLIENT_ID": "mcp-service",
        "GRAVITINO_OAUTH_CLIENT_SECRET": "<secret>",
        "GRAVITINO_OAUTH_SCOPE": "gravitino"
      }
    }
  }
}
```

In Cursor stdio mode the MCP process typically receives no `Authorization` header from the client. Set the `GRAVITINO_OAUTH_*` variables (or the matching CLI flags) so MCP fetches a service token with the `client_credentials` grant. Omit `env` to run anonymously, or use `--token` / `GRAVITINO_TOKEN` for a static credential instead.

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

| Tool name                           | Description                                                                    | Tag          |
|-------------------------------------|--------------------------------------------------------------------------------|--------------|
| `get_list_of_catalogs`              | Retrieve a list of all catalogs in the system.                                 | `catalog`    |
| `get_list_of_schemas`               | Retrieve a list of schemas belonging to a specific catalog.                    | `schema`     |
| `get_list_of_tables`                | Retrieve a list of tables within a specific catalog and schema.                | `table`      |
| `get_table_metadata_details`        | Retrieve comprehensive metadata details for a specific table.                  | `table`      |
| `list_of_models`                    | Retrieve a list of models within a specific catalog and schema.                | `model`      |
| `load_model`                        | Retrieve comprehensive metadata details for a specific model.                  | `model`      |
| `list_model_versions`               | Retrieve a list of versions for a specific model.                              | `model`      |
| `load_model_version`                | Retrieve comprehensive metadata details for a specific model version.          | `model`      |
| `load_model_version_by_alias`       | Retrieve comprehensive metadata details for a specific model version by alias. | `model`      |
| `metadata_type_to_fullname_formats` | Retrieve the metadata type to fullname formats mapping.                        | `metadata`   |
| `list_of_topics`                    | Retrieve a list of topics within a specific catalog and schema.                | `topic`      |
| `load_topic`                        | Retrieve comprehensive metadata details for a specific topic.                  | `topic`      |
| `list_of_filesets`                  | Retrieve a list of filesets within a specific catalog and schema.              | `fileset`    |
| `load_fileset`                      | Retrieve comprehensive metadata details for a specific fileset.                | `fileset`    |
| `list_files_in_fileset`             | Retrieve a list of files within a specific fileset.                            | `fileset`    |
| `list_of_jobs`                      | Retrieve a list of jobs                                                        | `job`        |
| `get_job_by_id`                     | Retrieve a job by its ID.                                                      | `job`        |
| `list_of_job_templates`             | Retrieve a list of job templates.                                              | `job`        |
| `get_job_template_by_name`          | Retrieve a job template by its name.                                           | `job`        |
| `run_job`                           | Run a job with the specified parameters.                                       | `job`        |
| `cancel_job`                        | Cancel a running job by its ID.                                                | `job`        |
| `get_tag_by_name`                   | Retrieve a tag by its name.                                                    | `tag`        |
| `list_of_tags`                      | Retrieve a list of tags.                                                       | `tag`        |
| `list_tags_for_metadata`            | Retrieve a list of tags associated with a specific metadata item.              | `tag`        |
| `list_metadata_by_tag`              | Retrieve a list of metadata items associated with a specific tag.              | `tag`        |
| `associate_tag_with_metadata`       | Associate tags with a specific metadata item.                                  | `tag`        |
| `disassociate_tag_from_metadata`    | Disassociate tags from a specific metadata item.                               | `tag`        |
| `list_statistics_for_metadata`      | Retrieve a list of statistics associated with a specific metadata item.        | `statistics` |
| `list_statistics_for_partition`     | Retrieve a list of statistics associated with a specific partition.            | `statistics` |
| `get_list_of_policies`              | Retrieve a list of policies in the system.                                     | `policy`     |
| `get_policy_detail_information`     | Retrieve detailed information for a specific policy by policy name.            | `policy`     |
| `list_policies_for_metadata`        | List all policies associated with a specific metadata item.                    | `policy`     |
| `list_metadata_by_policy`           | List all metadata items associated with a specific policy.                     | `policy`     |
| `get_policy_for_metadata`           | Get a policy associated with a specific metadata item.                         | `policy`     |


## Configuration

You could config Gravitino MCP server by arguments, `uv run mcp_server -h` shows the detailed information.

| Argument                         | Description                                                                                                                     | Default value               | Required |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------|-----------------------------|----------|
| `--metalake`                     | The Gravitino metalake name.                                                                                                    | none                        | Yes      |
| `--gravitino-uri`                | The URI of Gravitino server.                                                                                                    | `http://127.0.0.1:8090`     | No       |
| `--transport`                    | Transport protocol: stdio (local), http / streamable-http (Streamable HTTP).                                                    | `stdio`                     | No       |
| `--mcp-url`                      | The URL of MCP server if using HTTP transport.                                                                                  | `http://127.0.0.1:8000/mcp` | No       |
| `--token`                        | Static credential for Gravitino; or set `GRAVITINO_TOKEN`. See Authentication. Wins over OAuth client-credentials.              | none (anonymous)            | No       |
| `--oauth-token-endpoint`         | OAuth2 token URL for client-credentials. Or `GRAVITINO_OAUTH_TOKEN_ENDPOINT`.                                                   | none                        | No       |
| `--oauth-client-id`              | OAuth2 client id. Or `GRAVITINO_OAUTH_CLIENT_ID`.                                                                               | none                        | No       |
| `--oauth-client-secret`          | OAuth2 client secret. Or `GRAVITINO_OAUTH_CLIENT_SECRET`.                                                                       | none                        | No       |
| `--oauth-scope`                  | Optional OAuth2 scope. Or `GRAVITINO_OAUTH_SCOPE`.                                                                              | none                        | No       |
| `--no-service-identity-fallback` | HTTP only: reject requests with no `Authorization` when OAuth or `--token` is set. Or `GRAVITINO_NO_SERVICE_IDENTITY_FALLBACK`. | `false`                     | No       |
| `--tls-cert`                     | PEM certificate to serve the endpoint over HTTPS. Requires `--tls-key`.                                                         | none                        | No       |
| `--tls-key`                      | PEM private key to serve the endpoint over HTTPS. Requires `--tls-cert`.                                                        | none                        | No       |

## Authentication

By default the MCP server talks to Gravitino anonymously. There are three ways to authenticate MCP when calling Gravitino.

### Static startup token (stdio and HTTP)

Pass `--token` (or set the `GRAVITINO_TOKEN` environment variable) to authenticate the server with a static credential. The token is masked in the server's log output.

A bare value is treated as an OAuth2 token and sent as `Authorization: Bearer <token>`. A value that already begins with an HTTP authentication scheme is forwarded with that scheme preserved, so the credential can match whatever `gravitino.authenticators` the server is configured with:

| `--token` value             | `Authorization` header sent    |
|-----------------------------|--------------------------------|
| `abc`                       | `Bearer abc`                   |
| `Bearer abc`                | `Bearer abc`                   |
| `Basic dXNlcjpwYXNz`        | `Basic dXNlcjpwYXNz`           |
| `Custom credentials`        | `Custom credentials`           |
| empty or whitespace only    | none (anonymous)               |

The built-in scheme names (`Basic`, `Bearer`, `Negotiate`) are recognized case-insensitively and normalized to the capitalization Gravitino's authenticators expect; a custom scheme name is forwarded unchanged.

Because a bare token is only Bearer-prefixed when it carries no scheme, a static credential whose value contains a space and begins with a scheme-like word is interpreted as scheme plus credential. Quote such values with an explicit scheme (for example `--token "Bearer my secret"`) to keep them Bearer tokens.

```shell
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 --token <your-token>
# or, against a server configured with `gravitino.authenticators = basic`
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 --token "Basic $(printf '%s' 'user:password' | base64)"
# or
export GRAVITINO_TOKEN=<your-token>
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090
```

In `stdio` mode this token is used for every request. In HTTP mode it is only the fallback, used when an incoming request does not carry its own `Authorization` header. If both `--token` and OAuth client-credentials are set, `--token` wins.

### OAuth client credentials (service identity)

When Gravitino uses `gravitino.authenticators = oauth`, a pasted Bearer access token in `--token` expires and is not refreshed. For the **service** identity (Cursor stdio, or HTTP when the caller sends no `Authorization` header), configure MCP as an OAuth client of the same identity provider Gravitino trusts.

Set `--oauth-token-endpoint`, `--oauth-client-id`, and `--oauth-client-secret` together, plus optional `--oauth-scope` (or the matching `GRAVITINO_OAUTH_*` environment variables). MCP requests an access token with the `client_credentials` grant, caches it, refreshes before expiry, and retries once on HTTP 401.

In Cursor, put the `GRAVITINO_OAUTH_*` values in the `env` block of `~/.cursor/mcp.json` (see [Usage](#usage)). `--token` / `GRAVITINO_TOKEN` overrides OAuth client-credentials and stays static (no refresh). An incoming HTTP `Authorization` header is forwarded as-is and is not refreshed by MCP.

Gravitino maps the JWT to a metalake principal from claims configured in [`gravitino.authenticator.oauth.principalFields`](./security/how-to-authenticate.md#server-configuration) (often `sub`); that principal may differ from `--oauth-client-id`. It must exist as a metalake user with the needed grants, or tool calls fail with 403.

Prefer environment variables (or the `env` block in `~/.cursor/mcp.json`) for the client secret so it does not appear in `ps` output or shell history:

```shell
export GRAVITINO_OAUTH_TOKEN_ENDPOINT=https://idp.example/realms/gravitino/protocol/openid-connect/token
export GRAVITINO_OAUTH_CLIENT_ID=mcp-service
export GRAVITINO_OAUTH_CLIENT_SECRET=<secret>
export GRAVITINO_OAUTH_SCOPE=gravitino

uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090
```

The matching CLI flags (`--oauth-token-endpoint`, `--oauth-client-id`, `--oauth-client-secret`, `--oauth-scope`) work the same way, but avoid passing `--oauth-client-secret` on the command line in production.

This path does not replace per-request user identity in HTTP mode (see below).

### Per-request identity (HTTP)

When the server runs with HTTP transport, the `Authorization` header of each incoming MCP request is forwarded verbatim to Gravitino. The scheme is preserved, so OAuth2 (`Bearer`), Gravitino simple authentication (`Basic <base64(user:dummy)>`) and others all work. This keeps concurrent sessions from different principals isolated — one principal's identity never leaks into another's calls — and lets Gravitino enforce authorization per caller. The per-request header takes priority over the static `--token` and over OAuth client-credentials.

**Security warning:** When OAuth client-credentials or `--token` is configured, an HTTP request with **no** `Authorization` header is authenticated as the **service identity**, not as anonymous. With OAuth, that identity refreshes automatically and stays valid for a long time. If the MCP HTTP endpoint is reachable by more than one caller, anyone who omits `Authorization` receives the service principal's full permissions. Use stdio transport for single-user integrations (for example Cursor), bind HTTP to a trusted network, or place the endpoint behind a reverse proxy that requires authentication.

For exposed or multi-caller HTTP deployments, set `--no-service-identity-fallback` (or `GRAVITINO_NO_SERVICE_IDENTITY_FALLBACK=1`) so requests without `Authorization` are rejected instead of using the service identity. The flag is ignored for stdio transport.

Authorization itself is always enforced by Gravitino: the MCP server forwards the identity but does not make access-control decisions of its own.

### Serving over HTTPS (TLS)

To serve the MCP HTTP endpoint (the `--mcp-url`, not the `--gravitino-uri`) over TLS, provide both `--tls-cert` and `--tls-key` and use an `https://` `--mcp-url`. The certificate and key must be provided together, and the URL scheme must match the TLS setting (an `https://` URL without a cert/key, or a cert/key behind an `http://` URL, is rejected at startup).

```shell
uv run mcp_server --metalake test --gravitino-uri http://127.0.0.1:8090 \
  --transport streamable-http --mcp-url https://localhost:8000/mcp \
  --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```

## Audit Logging

Every tool invocation is recorded as one structured JSON line in `gravitino-mcp-audit.log` (written to the server's working directory). Each record is attributed to the incoming HTTP `Authorization` header when present; otherwise to the configured service identity (`--token` or OAuth client id).

| Field        | Description                                                                                                                                                                             |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `timestamp`  | UTC ISO-8601 time of the call.                                                                                                                                                          |
| `principal`  | Caller identity: username for `Basic` simple auth, `bearer:<first-8-chars>` for a Bearer token, `oauth:<first-8-chars-of-client-id>` when OAuth client-credentials is the service identity (stdio or HTTP with no caller header), or `anonymous` when no identity is present. |
| `tool`       | Name of the invoked MCP tool.                                                                                                                                                           |
| `outcome`    | `allow` for successful calls, `deny` for failed ones. `deny` is emitted for any tool-call exception (authorization denial being the common case); inspect `error_type` to disambiguate. |
| `error_type` | Exception class name, present only when `outcome` is `deny`.                                                                                                                            |

Example record:

```json
{"timestamp": "2026-06-16T03:21:09.123456+00:00", "principal": "alice", "tool": "get_list_of_catalogs", "outcome": "allow"}
```
