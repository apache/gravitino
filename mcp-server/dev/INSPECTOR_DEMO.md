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

# Hands-on MCP Authorization Demo (with MCP Inspector)

Drive the Gravitino MCP server interactively through the
[MCP Inspector](https://github.com/modelcontextprotocol/inspector) and watch
per-user authorization and audit work end to end, as two principals
(`admin` and `bob`).

This exercises the three governance moments:

1. **Scoped discovery** — `admin` and `bob` run the same call and get different,
   authorization-scoped results.
2. **Write denied** — `bob` (read-only) attempts a write and is denied by
   Gravitino authorization, surfaced as an explicit error through MCP.
3. **Audit trail** — every call produces an audit record attributed to the
   correct principal with an allow/deny outcome.

---

## 1. Prerequisites

- A built Gravitino distribution. If you don't have one:
  ```bash
  ./gradlew compileDistribution -x test -PskipWeb=true
  # produces distribution/package/
  ```
- `uv` available in the `mcp-server/` directory (the project already uses it).
- Node.js (the start script launches the Inspector via `npx @modelcontextprotocol/inspector`).
- If you run behind an HTTP proxy, the scripts already export
  `NO_PROXY=localhost,127.0.0.1`; make sure your shell doesn't force the proxy
  for loopback in some other way.

---

## 2. Start the demo environment

From the `mcp-server/` directory:

```bash
./dev/start_inspector_demo.sh
```

This script (idempotent — safe to re-run):

1. Enables `simple` auth + authorization in `distribution/package/conf/gravitino.conf`
   (backing up the original).
2. Starts Gravitino (or reuses a running one).
3. Provisions demo data into metalake **`mcp_authz_it`**:
   - `cat_allowed` and `cat_denied` (two model catalogs)
   - user **`bob`**
   - a reader role granting `bob` `USE_CATALOG` on `cat_allowed` **only**
4. Starts the MCP server in HTTP mode at `http://127.0.0.1:8000/mcp`.
5. Starts the **MCP Inspector** at `http://localhost:6274/` (with the session-token
   requirement disabled, so the plain URL works directly).

On success it prints the connection details and the two principals' tokens. You
should see the provisioning summary confirming the different slices:

```
[demo]   admin sees catalogs: ['cat_allowed', 'cat_denied']
[demo]   bob sees catalogs: ['cat_allowed']
```

> If a distribution isn't found, set `GRAVITINO_HOME=/path/to/distribution`.

---

## 3. Open the Inspector

The start script already launched it. Just open:

```
http://localhost:6274/
```

(No session token needed — the script starts it with `DANGEROUSLY_OMIT_AUTH=true`
for local convenience.)

### Connect

| Field          | Value                                    |
|----------------|------------------------------------------|
| Transport Type | `Streamable HTTP`                        |
| URL            | `http://127.0.0.1:8000/mcp`              |
| Header Name    | `Authorization`                          |
| Header Value   | `Basic YWRtaW46ZHVtbXk=`  (this is `admin`) |

The two principal tokens (these are Gravitino simple-auth headers, i.e.
`Basic base64("<user>:dummy")`):

| Principal | Authorization header value     |
|-----------|--------------------------------|
| `admin`   | `Basic YWRtaW46ZHVtbXk=`       |
| `bob`     | `Basic Ym9iOmR1bW15`           |

Click **Connect**, then **List Tools** — you should see the full read + write
tool surface (catalogs, schemas, tables, filesets, topics, models, tags, …).

> **Identity is the header.** The Inspector sends your `Authorization` header on
> every request; the MCP server forwards it verbatim to Gravitino, which
> authorizes against that principal. To switch principals, change the header
> value and reconnect.

---

## 4. The three scenarios

Keep a terminal tailing the audit log while you click:

```bash
tail -f gravitino-mcp-audit.log
```

### Scenario 1 — Scoped discovery

1. Connected as **admin**, run tool **`get_list_of_catalogs`**.
   → Returns **both** `cat_allowed` and `cat_denied`.
2. Reconnect with the **bob** header (`Basic Ym9iOmR1bW15`), run
   **`get_list_of_catalogs`** again.
   → Returns **only** `cat_allowed`.

Same call, different results — sourced entirely from Gravitino's list filtering,
not from any logic in the MCP server.

### Scenario 2 — Write denied by authorization

Still connected as **bob**, run **`create_tag`** with arguments:

```json
{ "name": "test_tag", "comment": "x", "properties": {} }
```

→ You get an explicit error, e.g.
`User 'bob' is not authorized to perform operation 'createTag' on metadata 'mcp_authz_it'`.

It's a real authorization denial, not a hidden tool or a silent no-op. Reconnect
as **admin** and run the same `create_tag` — it succeeds (admin owns the metalake).

### Scenario 3 — Audit trail

Look at the audit log you've been tailing. You should see discrete, correctly
attributed records, for example:

```json
{"timestamp": "...", "principal": "admin", "tool": "get_list_of_catalogs", "outcome": "allow"}
{"timestamp": "...", "principal": "bob",   "tool": "get_list_of_catalogs", "outcome": "allow"}
{"timestamp": "...", "principal": "bob",   "tool": "create_tag", "outcome": "deny", "error_type": "McpError"}
```

`admin`'s reads attributed to `admin`, `bob`'s reads to `bob`, and `bob`'s denied
write to `bob` with a `deny` outcome.

---

## 5. Tear down

```bash
./dev/stop_inspector_demo.sh
```

Stops the Inspector, the MCP server, and Gravitino, and restores the original
`gravitino.conf`.

---

## 6. Troubleshooting

**The audit log looks empty / stuck at 0 bytes.**
The MCP server opens the audit file once at startup and holds the handle. If you
`rm` the file while the server is running, the process keeps writing to the now
unlinked inode and a new empty file appears at the path — so you see nothing.
Don't delete it mid-run; truncate instead (`: > gravitino-mcp-audit.log`), or
restart the server. The start script truncates rather than deletes for exactly
this reason.

**`HTTP 000` / `502` when curling localhost.**
An HTTP proxy is intercepting loopback traffic. Export
`NO_PROXY=localhost,127.0.0.1` (the scripts already do) or pass `curl --noproxy '*'`.

**MCP server fails to bind (`can't assign requested address`).**
`localhost` resolved to a non-loopback address. The scripts bind to the
`127.0.0.1` literal to avoid this.

**`ModuleNotFoundError: No module named 'mcp_server'`.**
Launch with `uv run python -m mcp_server ...` (the scripts do); `uv run mcp_server`
needs an editable install.

**Re-running the start script.**
It's idempotent: it reuses a running Gravitino/MCP server and drops+recreates the
demo metalake, so you can re-run it freely.
