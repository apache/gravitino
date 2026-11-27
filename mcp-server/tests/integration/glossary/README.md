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

The domain, hierarchy and concurrency files call the Gravitino REST API directly
(31 cases). Only `test_mcp.py` calls the MCP endpoint (one case). The separate
[REST contract checks and open findings](../../../../dev/validation/glossary/README.md)
cover request types, media negotiation, missing metalakes and a MySQL deadlock.

Run these tests against two Gravitino servers that share an initialized relational
store. Each test creates a uniquely named metalake and removes it after the test.
Both servers need the `lakehouse-generic` catalog provider and the following
configuration in `gravitino.conf`:

```properties
gravitino.authenticators = simple
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = admin
```

From `mcp-server/`, set the two server URLs and run the suite:

```sh
export GRAVITINO_URI=http://127.0.0.1:8090
export GRAVITINO_PEER_URI=http://127.0.0.1:8091
uv run pytest -q -rs tests/integration/glossary
```

To include the MCP case, start an HTTP MCP server with the glossary tools against
`GRAVITINO_URI` and export `MCP_URL` (for example, `http://127.0.0.1:8000/mcp`)
before running pytest. The test passes its metalake name in each tool call.
Without `MCP_URL`, pytest skips that case and runs the REST cases.

The suite exercises domain inheritance and explicit denial, table, column and
metric bindings in both directions, and multi-parent BROADER and NARROWER
traversal. It checks the 499, 500 and 501 element batch boundaries and uses the
second server to check visibility and atomicity during concurrent writes.
