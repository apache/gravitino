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

---
name: trino-test
description: Run, debug, and manage Trino integration tests for the Gravitino project.
argument-hint: "[command or intent] (e.g. run all | test mysql | trino 446 | add test)"
allowed-tools: Bash
disable-model-invocation: false
---

# /trino-test — Trino Integration Test Skill

This skill helps you **run, debug, and manage Trino integration tests** for the **Gravitino** project.

Use this skill whenever the user asks about:
- running Trino integration tests
- testing specific connectors (e.g. MySQL)
- testing specific Trino versions
- adding or fixing Trino test cases

---

## Documentation Reference

Full guide:

.claude/skills/trino-test-guide.md

Read the guide for complete details on:
- Test architecture and structure
- All available parameters and options
- Test modes (`--auto=all | gravitino | none`)
- Adding and modifying tests
- Expected output format (`.txt` with `%` wildcards)
- Debugging and troubleshooting

---

## Project Root

All commands assume:

```bash
cd /home/ubuntu/git/gravitino
```

---

## Quick Commands

### Run all tests
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all
```

### Run specific test set
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql
```

### Run specific test file
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql --tester_id=00004
```

### Run specific test catalog with testset
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all  --test_set=tpch --catalog=hive


### Test specific Trino version with specific trino connector
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all \
  --trino_version=<VERSION> \
  --trino_connector_dir=<WORKSPACE>/trino-connector/trino-connector-<VERSION_RANGE>/build/libs
```

## Check test status

grep -E "(Test progress|All testers|Total|PASS|FAIL|BUILD) from log

## Test Structure

Location:

```
trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/
```

Each test consists of:
- `*.sql` — SQL statements to execute
- `*.txt` — Expected output
  - Supports `%` wildcard for flexible matching
