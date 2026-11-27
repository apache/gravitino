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

# Glossary integration tests

Run the suite from `mcp-server/`. With a primary server, a peer and an MCP
endpoint configured, pytest runs all 58 cases through one command:

```sh
export GRAVITINO_URI=http://127.0.0.1:8090
export GRAVITINO_PEER_URI=http://127.0.0.1:8091
export MCP_URL=http://127.0.0.1:8000/mcp
uv run pytest -q -rs tests/integration/glossary
```

- The 26 cases in `test_rest_contracts.py` use only `GRAVITINO_URI`. They check
  request types, media negotiation and missing-metalake responses.
- The 31 cases in `test_domains.py`, `test_hierarchy.py` and `test_concurrency.py`
  use both Gravitino servers against one relational store. They check domain
  access, reverse bindings, multi-parent traversal, batch boundaries at 499,
  500 and 501 entries, and consistency during concurrent writes.
- `test_mcp.py` uses the same two-server fixture and an HTTP MCP server connected
  to the primary. It resolves business terms and follows their bindings.

Set only the URLs available for a run. For example, with only `GRAVITINO_URI`
set, the same command runs the 26 REST contract cases and skips the other 32.
All fixtures use simple authentication and create and clean up their own metalakes.

For the two-server tests, install the `lakehouse-generic` catalog provider on both
servers and set the following in each `gravitino.conf`:

```properties
gravitino.authenticators = simple
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = admin
```

For the single-server REST checks, `GRAVITINO_USER` selects the service admin
(default `admin`). To verify the authorization-disabled responses, run against
a primary configured with `gravitino.authorization.enable=false`, set
`GRAVITINO_AUTHORIZATION_ENABLED=false` and leave the peer and MCP URLs unset.

## Regression results

All 26 REST contract checks passed on H2, PostgreSQL and MySQL with authorization
enabled, and on H2 with authorization disabled. Before the fixes, the same checks
reported eight failures for accepted inputs and six for success response headers.

The baseline for the four regression cases below was
`glossary-term-dev` at `5b9bbc0d2e` with the MCP tools from #15402.
The REST sweep covered all 11 glossary operations on H2 2.2.224, MySQL 8.0.35
and PostgreSQL 15.19. All 1,896 recorded response bodies passed their OpenAPI
schema checks (632 per backend). Separate checks of the request types, response
headers, error semantics and concurrent writes exposed the following issues.
See the [glossary OpenAPI specification](../../../../docs/open-api/terms.yaml).

### 1. Create and alter require JSON strings

In a new metalake, `POST /terms` with `{"name":123}` returned 200 and created
the approved term `"123"`. For an existing term with definition `"original"`
at version 1, the following update also returned 200:

```http
PUT /api/metalakes/{metalake}/terms/BadInput
Content-Type: application/json

{"updates":[{"@type":"updateDefinition","newDefinition":false}]}
```

A separate GET returned `"definition":"false"` and `"version":2`. The schemas
require JSON strings for these inputs. The checks expect HTTP 400, verify that
a rejected term cannot be read back, and compare the entire term after a
failed edit, including its revision token. Five create cases cover the name,
definition, synonym, property value and source reference. Three edit cases cover
the definition, synonym and property value. These checks now pass. The request
DTOs use strict string deserialization for scalar fields, collection elements
and map values. An explicit null still clears a nullable definition or source reference.

### 2. Term operations honor the requested v1 response media type

With a term named `Wire`, send `GET /terms/Wire` with
`Accept: application/vnd.gravitino.v1+json`. The response was 200 with
`Content-Type: application/json`. List, create, load, alter, drop and hierarchy
all returned this header. The OpenAPI success responses declare
`application/vnd.gravitino.v1+json`. All five binding operations returned
the requested type in the same environment. The runnable checks exercise the
six affected operations and use `GET /terms/Wire/objects` as a control. The term
resource now lets Jersey negotiate the response type, including plain JSON.

### 3. Missing-metalake responses depend on authorization

Create a metalake and a term, drop the metalake, and repeat the glossary
requests as the same principal. With authorization enabled, all 11 operations
returned 403 `ForbiddenException` on all three backends. With authorization
disabled, all 11 returned 404 `NoSuchMetalakeException` on H2. As a control,
dropping an absent term in a live metalake returned 200 with `dropped: false`.

The OpenAPI entry for `dropTerm` and the shared 403 description now document the
metalake membership check. The tests assert both the HTTP status and the typed
error returned for each operation.

### 4. MySQL owner cleanup is scoped to the metalake

Two test suites overlapped on the same MySQL store. In one metalake a term
update adding a domain returned 200, while a concurrent delete of that domain
returned 500. The server logged a MySQL transaction rollback in
`OwnerMetaMapper.softDeleteOwnerRelByMetadataObjectIdAndType`.

InnoDB identified the two transactions that took locks on `owner_meta`:

- Schema cleanup held `idx_owner_meta_del_upd_obj` and waited for `PRIMARY`.
  Its schema ID belonged to the other suite's metalake being dropped.
- Domain cleanup held `PRIMARY` and waited for `idx_owner_meta_del_upd_obj`.
  Its domain ID belonged to the metalake running the term update. InnoDB rolled
  back the domain cleanup.

Schema and catalog cascades now pass the metalake ID to the owner cleanup query.
On MySQL these cascades and metalake deletion use the index on
`owner_meta.metalake_id`. Fresh installs and the 1.3.0-to-2.0.0 migration both
create the index. The storage regression holds an owner row lock in another
metalake while schema, catalog and metalake cleanup each remove their own owners.
With the same unrelated row locked on MySQL, the baseline schema cleanup timed
out and the fixed query updated exactly one target owner. Two overlapping
two-server runs on one MySQL database passed all 64 cases with zero InnoDB
deadlocks.

The [two-server concurrency test](test_concurrency.py)
also verifies both legal outcomes of a domain deletion racing a term edit: a
successful edit followed by a 409 on deletion, or a successful deletion and a
404 on adding the removed domain.
