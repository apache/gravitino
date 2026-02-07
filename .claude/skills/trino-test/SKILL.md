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

### Test with Trino 446
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all \
  --trino_version=446 \
  --trino_connector_dir=/home/ubuntu/git/gravitino/trino-connector/trino-connector-446-451/build/libs
```

### Multi-version test
```bash
./trino-connector/integration-test/trino-test-tools/run_test_with_versions.sh \
  --trino_versions_map="446:trino-connector-446-451"
```

---

## Test Structure

Location:

```
trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/
```

Each test consists of:
- `*.sql` — SQL statements to execute
- `*.txt` — Expected output
  - Supports `%` wildcard for flexible matching
