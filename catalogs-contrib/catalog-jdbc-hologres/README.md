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

# Hologres JDBC Catalog

## Integration Tests

Since Hologres is a cloud service hosted on Alibaba Cloud, integration tests require a real Hologres instance. These tests are disabled by default and only run when the appropriate environment variables are set.

### Prerequisites

- A running Hologres instance on Alibaba Cloud
- A database created in the Hologres instance
- Credentials with sufficient permissions to create/drop schemas and tables

### Environment Variables

| Variable                      | Required | Description                                                          |
|:------------------------------|:---------|:---------------------------------------------------------------------|
| `GRAVITINO_HOLOGRES_JDBC_URL` | Yes      | Hologres JDBC URL, e.g. `jdbc:postgresql://<host>:<port>/<database>` |
| `GRAVITINO_HOLOGRES_USERNAME` | Yes      | Hologres access key ID or username                                   |
| `GRAVITINO_HOLOGRES_PASSWORD` | Yes      | Hologres access key secret or password                               |

The tests are automatically enabled when all three environment variables are set and non-blank.

### Running Integration Tests

```bash
# Set environment variables
export GRAVITINO_HOLOGRES_JDBC_URL="jdbc:postgresql://<host>:<port>/<database>"
export GRAVITINO_HOLOGRES_USERNAME="<your-username>"
export GRAVITINO_HOLOGRES_PASSWORD="<your-password>"

# Run integration tests
./gradlew :catalogs-contrib:catalog-jdbc-hologres:test
```

### Running Unit Tests Only

To skip integration tests and run only unit tests:

```bash
./gradlew :catalogs-contrib:catalog-jdbc-hologres:test -PskipITs
```

### Test Coverage

The integration tests cover:

- **Schema operations**: create, load, list, drop schemas
- **Table CRUD**: create, load, alter (rename, add/delete/rename columns, update comments), drop tables
- **Type conversion**: bool, int2, int4, int8, float4, float8, text, varchar, date, timestamp, timestamptz, numeric, bytea
- **Distribution**: hash distribution key
- **Table properties**: orientation, time_to_live_in_seconds, etc.
- **Primary key index**: single and composite primary keys
- **Column default values**: integer, varchar, boolean literals
- **Schema comments**: create schema with comment
