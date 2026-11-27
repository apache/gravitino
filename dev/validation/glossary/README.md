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

# Glossary REST API validation

Four open findings came out of the validation of #14913 on
`glossary-term-dev` at `5b9bbc0d2e` with the MCP tools from #15402.
The REST sweep covered all 11 glossary operations on H2 2.2.224, MySQL 8.0.35
and PostgreSQL 15.19. All 1,896 recorded response bodies passed their OpenAPI
schema checks (632 per backend). Separate checks of the request types, response
headers, error semantics and concurrent writes exposed the following issues.
See the [glossary OpenAPI specification](../../../docs/open-api/terms.yaml).

## 1. Creating or altering a term coerces numbers and booleans into strings

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
the definition, synonym and property value.

## 2. Six operations ignore the requested v1 response media type

With a term named `Wire`, send `GET /terms/Wire` with
`Accept: application/vnd.gravitino.v1+json`. The response was 200 with
`Content-Type: application/json`. List, create, load, alter, drop and hierarchy
all returned this header. The OpenAPI success responses declare
`application/vnd.gravitino.v1+json`. All five binding operations returned
the requested type in the same environment. The runnable checks exercise the
six affected operations and use `GET /terms/Wire/objects` as a control.

## 3. The missing-metalake documentation omits the authorization-dependent 403

Create a metalake and a term, drop the metalake, and repeat the glossary
requests as the same principal. With authorization enabled, all 11 operations
returned 403 `ForbiddenException` on all three backends. With authorization
disabled, all 11 returned 404 `NoSuchMetalakeException` on H2. As a control,
dropping an absent term in a live metalake returned 200 with `dropped: false`.

The OpenAPI entry for `dropTerm` describes a missing metalake under 404 and
describes 403 as an insufficient privilege. Document the membership check that
can return 403 for a missing metalake. The checks verify both configurations
so the documentation can be updated without changing the authorization behavior.

## 4. Concurrent cleanup in different metalakes can deadlock on MySQL

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

The [concurrent domain-delete case](../../../mcp-server/tests/integration/glossary/test_concurrency.py)
expects either a successful update and a 409 on deleting a nonempty domain, or a
successful deletion and a 404 on adding the removed domain. It failed in the
overlapping run (31/32 passed). All 32 passed when rerun alone on the same
MySQL store. Reproduction depends on the overlap with a separate metalake
teardown. Capture `SHOW ENGINE INNODB STATUS` immediately after a failure.

## Run the REST checks

Use a disposable Gravitino server with simple authentication and a service admin.
The checks create uniquely named metalakes and remove them on teardown.
From the repository root, using a Python environment with `httpx` and `pytest`:

```sh
GRAVITINO_URI=http://127.0.0.1:8090 GRAVITINO_AUTHORIZATION_ENABLED=true \
  python -m pytest -q dev/validation/glossary/check_rest_contracts.py
```

Set `GRAVITINO_USER` if the service admin differs from `admin`. To verify the
authorization-disabled control, point the command at a server started with
`gravitino.authorization.enable=false` and set
`GRAVITINO_AUTHORIZATION_ENABLED=false`.

At the tested revision, each run produced **14 failures and 12 passes**. Eight
failures identify accepted malformed inputs, and six identify response headers.
The binding control and all 11 missing-metalake checks passed. The command
exits nonzero while the two wire contract bugs remain. It requires one Gravitino
endpoint. For domain bindings, graph traversal, and cross-server concurrency,
run the separate [two-server suite](../../../mcp-server/tests/integration/glossary/README.md).
