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

# Iceberg expression string to read restrictions

This small demo converts an Iceberg **expression JSON string** into the
`read-restrictions.required-row-filter` shape proposed in
[Iceberg PR #13879](https://github.com/apache/iceberg/pull/13879). The input is Iceberg's
expression JSON format, not SQL text.

Iceberg 1.11, the version used by this repository, parses the older `term`/`value` predicate
form. The proposed read restriction uses the newer `left`/`right` form with a field ID.
`ExpressionParser` alone cannot emit that new form, so the demo binds the parsed expression
with `Binder` and constructs the new JSON for one supported case: equality on a string field.
The current Iceberg dependency also does not include the proposed `ReadRestrictions` Java model.

The example schema contains `region` with field ID `14`. Given this input string:

```json
{"type":"eq","term":"region","value":"US"}
```

the converter returns:

```json
{"read-restrictions":{"required-row-filter":{"type":"eq","left":{"type":"reference","id":14},"right":{"type":"literal","value":"US"}}}}
```

The field ID keeps the restriction attached to the same column after a rename. An unknown
column, a non-string column, or another predicate type fails explicitly. The demo does not
attach the JSON to a live `loadTable` response or enforce the filter in a reader.

Run the example assertions from the worktree root:

```shell
./gradlew :iceberg:iceberg-rest-server:test \
  --tests org.apache.gravitino.iceberg.service.rest.TestReadRestrictionExpressionDemo \
  -PskipITs
```

Source: `iceberg/iceberg-rest-server/src/test/java/org/apache/gravitino/iceberg/service/rest/ReadRestrictionExpressionDemo.java`.
