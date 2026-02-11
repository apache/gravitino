# Trino Integration Test Guide

## Overview

Gravitino's Trino integration tests are executed through the **TrinoQueryTestTool** class, which reads SQL files from testsets, executes SQL statements, and verifies output results. The tests support multiple modes and flexible configuration options.

## Part 1: How to Run Tests

### Test Architecture

```
trino-connector/integration-test/
├── src/test/
│   ├── java/
│   │   └── .../TrinoQueryTestTool.java          # Core test tool class
│   └── resources/
│       └── trino-ci-testset/testsets/           # Test sets directory
│           ├── jdbc-mysql/                       # MySQL test set
│           │   ├── 00001_select_table.sql       # SQL test file
│           │   └── 00001_select_table.txt       # Expected output file
│           ├── jdbc-postgresql/                  # PostgreSQL test set
│           ├── lakehouse-iceberg/                # Iceberg test set
│           ├── hive/                             # Hive test set
│           ├── tpch/                             # TPC-H test set
│           └── tpcds/                            # TPC-DS test set
└── trino-test-tools/                            # Test scripts
    ├── trino_integration_test.sh                # Main test script
    └── run_test_with_versions.sh                # Multi-version test script
```

### 1.1 Running Tests with Scripts (Recommended)

#### Basic Usage

```bash
# Run all tests (auto-start all services)
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh --auto=all

# Run all tests and ignore failures
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --ignore_failed
```

#### Specify Test Set

```bash
# Run specific test set (e.g., jdbc-mysql)
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql

# Run specific test set under specific catalog
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=tpch --catalog=mysql
```

#### Specify Test File

```bash
# Run specific test file (by tester_id prefix)
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql --tester_id=00004

# This will run jdbc-mysql/00004_query_pushdown.sql
```

#### Specify Trino Version

```bash
# Test with Trino 452
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --trino_version=452 \
  --trino_connector_dir=/path/to/trino-connector-452-468/build/libs

# Test with Trino 435
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --trino_version=435 \
  --trino_connector_dir=/path/to/trino-connector-435-439/build/libs
```

### 1.2 Test Modes

#### Auto Mode (--auto)

**all (default)**: Auto-start all services
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh --auto=all
```
- Auto-start Gravitino server
- Auto-start Docker containers (Trino, Hive, MySQL, PostgreSQL, etc.)
- Suitable for local development and CI environments

**gravitino**: Start only Gravitino server
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh --auto=gravitino
```
- Auto-start Gravitino server
- Requires manual start of other services
- Suitable for debugging Gravitino server

**none**: Don't auto-start any services
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=none \
  --gravitino_uri=http://10.3.21.12:8090 \
  --trino_uri=http://10.3.21.12:8080 \
  --mysql_uri=jdbc:mysql://10.3.21.12:3306
```
- Connect to running services
- Suitable for connecting to remote test environments

### 1.3 Advanced Options

#### Distributed Cluster Testing

```bash
# Use 3 independent Trino worker nodes
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --trino_worker_num=3
```

#### Generate Expected Output Files

```bash
# Generate expected output files for tests (for creating new tests)
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql --gen_output
```

#### Parameter Substitution

```bash
# Replace ${key} variables in test files
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --params=key1,value1;key2,value2
```

### 1.4 TrinoQueryTestTool Complete Parameter List

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--auto` | Auto-start mode: all/gravitino/none | all | `--auto=all` |
| `--ignore_failed` | Ignore failed tests and continue | false | `--ignore_failed` |
| `--gen_output` | Generate expected output files | false | `--gen_output` |
| `--test_host` | Host address for all services | 127.0.0.1 | `--test_host=10.3.21.12` |
| `--gravitino_uri` | Gravitino server URL | - | `--gravitino_uri=http://localhost:8090` |
| `--trino_uri` | Trino URL | - | `--trino_uri=http://localhost:8080` |
| `--hive_uri` | Hive metastore URL | - | `--hive_uri=thrift://localhost:9083` |
| `--mysql_uri` | MySQL JDBC URL | - | `--mysql_uri=jdbc:mysql://localhost:3306` |
| `--postgresql_uri` | PostgreSQL JDBC URL | - | `--postgresql_uri=jdbc:postgresql://localhost:5432` |
| `--hdfs_uri` | HDFS URL | - | `--hdfs_uri=hdfs://localhost:9000` |
| `--test_sets_dir` | Test sets directory | src/test/resources/trino-ci-testset/testsets | `--test_sets_dir=/path/to/testsets` |
| `--test_set` | Specify test set name | - | `--test_set=jdbc-mysql` |
| `--tester_id` | Specify test file prefix | - | `--tester_id=00004` |
| `--catalog` | Specify catalog name | - | `--catalog=mysql` |
| `--params` | Parameter substitution | - | `--params=key1,v1;key2,v2` |
| `--trino_worker_num` | Number of Trino workers | 0 | `--trino_worker_num=3` |
| `--trino_version` | Trino version | 452 | `--trino_version=452` |
| `--trino_connector_dir` | Connector JAR directory | trino-connector/build/libs | `--trino_connector_dir=/path/to/libs` |
| `--help` | Show help message | - | `--help` |

### 1.5 Common Test Scenarios

#### Scenario 1: Quick validation of all tests

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh --auto=all
```

#### Scenario 2: Test only MySQL functionality

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql
```

#### Scenario 3: Debug specific test file

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all --test_set=jdbc-mysql --tester_id=00004
```

#### Scenario 4: Verify Trino 452 compatibility

```bash
./trino-connector/integration-test/trino-test-tools/run_test_with_versions.sh \
  --trino_versions_map="452:trino-connector-452-468"
```

#### Scenario 5: Connect to remote test environment

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=none \
  --gravitino_uri=http://remote-server:8090 \
  --trino_uri=http://remote-server:8080 \
  --mysql_uri=jdbc:mysql://remote-server:3306 \
  --test_set=jdbc-mysql
```

## Part 2: How to Add/Modify Tests

### 2.1 Test File Structure

Each test consists of two files:

1. **SQL file** (`.sql`): Contains SQL statements to execute
2. **Expected output file** (`.txt`): Contains expected output for each SQL statement

### 2.2 Adding New Tests

#### Step 1: Create SQL Test File

Create SQL file in the appropriate test set directory with format: `{number}_{test_name}.sql`

```bash
# Example: Create new query test
vi trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/jdbc-mysql/00010_new_query.sql
```

SQL file content example:
```sql
-- Create schema
CREATE SCHEMA gt_mysql.test_db;

-- Use schema
USE gt_mysql.test_db;

-- Create table
CREATE TABLE users (
   id bigint NOT NULL,
   name varchar(50) NOT NULL,
   email varchar(100) NOT NULL
);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

-- Query data
SELECT * FROM users ORDER BY id;

-- Cleanup
DROP TABLE users;
DROP SCHEMA test_db;
```

#### Step 2: Generate Expected Output File

Use `--gen_output` option to auto-generate expected output:

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all \
  --test_set=jdbc-mysql \
  --tester_id=00010 \
  --gen_output
```

This will generate `00010_new_query.txt` file containing actual execution output.

#### Step 3: Verify and Adjust Expected Output

Check the generated `.txt` file:
```bash
cat trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/jdbc-mysql/00010_new_query.txt
```

Adjust expected output as needed, using wildcard `%` to match variable content.

### 2.3 Modifying Existing Tests

#### Modify SQL File

Directly edit the `.sql` file:
```bash
vi trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/jdbc-mysql/00004_query_pushdown.sql
```

#### Update Expected Output

Method 1: Regenerate
```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all \
  --test_set=jdbc-mysql \
  --tester_id=00004 \
  --gen_output
```

Method 2: Manual edit
```bash
vi trino-connector/integration-test/src/test/resources/trino-ci-testset/testsets/jdbc-mysql/00004_query_pushdown.txt
```

### 2.4 Expected Output File Format

#### Basic Format

Expected output file contains expected output for each SQL statement, separated by blank lines:

```
CREATE SCHEMA

USE

CREATE TABLE

INSERT: 2 rows

"1","Alice","alice@example.com"
"2","Bob","bob@example.com"

DROP TABLE

DROP SCHEMA
```

#### Using Wildcards

Use `%` to match variable content:

**Example 1: Match version number**
```
"Trino version: %
```
Matches: `Trino version: 452`, `Trino version: 435`, etc.

**Example 2: Match query plan**
```
%TableScan[table = gt_mysql:gt_db1.customer%]
```
Matches any TableScan node containing this table

**Example 3: Match LIKE operation**
```
%ScanFilter[table = gt_mysql:gt_db1.customer%like%phone%]
```
Ensures output contains `like` and `phone`, verifying query pushdown functionality

#### Quote Rules

For multi-line output (like EXPLAIN results), wrap with double quotes:

```
"Trino version: %
%
    %TableScan[table = gt_mysql:gt_db1.customer%]
           Layout: [custkey:bigint, name:varchar(25)]
%
"
```

### 2.5 Testing Best Practices

#### 1. Test Independence

Each test should be independent, not depending on other tests:
```sql
-- Good practice: Create and cleanup own resources
CREATE SCHEMA test_db;
USE test_db;
CREATE TABLE test_table (...);
-- Execute test
DROP TABLE test_table;
DROP SCHEMA test_db;
```

#### 2. Use Meaningful Test Names

```
00001_select_table.sql          # Basic query test
00004_query_pushdown.sql        # Query pushdown test
00008_update_table.sql          # UPDATE operation test
```

#### 3. Expected Output Flexibility

Use wildcards to match variable content, but retain key information for verification:

```
# Too strict (will fail due to version changes)
└─ ScanFilter[table = gt_mysql:gt_db1.customer, filterPredicate = "$like"("phone", ...)]

# Too loose (cannot verify functionality)
%

# Appropriate (verifies key functionality, allows format changes)
%ScanFilter[table = gt_mysql:gt_db1.customer%like%phone%]
```

#### 4. Test Data Volume

Use appropriate amount of test data:
```sql
-- Good practice: Small but sufficient data
INSERT INTO customer VALUES (1, 'Alice', ...);
INSERT INTO customer VALUES (2, 'Bob', ...);

-- Avoid: Too much test data
INSERT INTO customer SELECT * FROM large_table; -- May cause slow tests
```

### 2.6 Debugging Tests

#### View Test Execution Logs

Test logs output to console with detailed execution information:
```
2026-02-06 21:17:25 INFO  [pool-7-thread-1] TrinoQueryIT:245 -
Execute sql in the tester jdbc-mysql/00004_query_pushdown.sql under catalog mysql successfully.
```

#### Compare Actual vs Expected Output

When test fails, logs show:
```
Failed to execute test java.lang.RuntimeException:
Execute sql in the tester jdbc-mysql/00004_query_pushdown.sql under catalog mysql failed.
Sql:
explain select * from customer where phone like '%2342%' limit 10;
Expect:
"Trino version: %
%
    %ScanFilter[...]
"
Actual:
"Trino version: 452
Fragment 0 [SINGLE]
    ...
"
```

#### Run Failed Test Separately

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all \
  --test_set=jdbc-mysql \
  --tester_id=00004
```

#### Use ignore_failed to Continue Testing

```bash
./trino-connector/integration-test/trino-test-tools/trino_integration_test.sh \
  --auto=all \
  --ignore_failed
```

#### Check Log Files

When tests fail or you need to debug issues, check the following log locations:

**Test Execution Logs:**
```bash
# Integration test logs (test execution output)
cat integration-test-common/build/integration-test-common-integration-test.log
```

**Container Logs:**
```bash
# Trino container logs
cat integration-test-common/build/trino-ci-container-log/trino.log

# Hive container logs (directory containing multiple log files)
ls integration-test-common/build/trino-ci-container-log/hive/
cat integration-test-common/build/trino-ci-container-log/hive/<log-file>

# HDFS container logs (directory containing multiple log files)
ls integration-test-common/build/trino-ci-container-log/hdfs/
cat integration-test-common/build/trino-ci-container-log/hdfs/<log-file>
```

These logs contain detailed information about:
- Test execution results and failures
- SQL query execution details
- Container startup and runtime issues
- Service connection errors
- Docker container output

### 2.7 Test Set Organization

#### Organize by Functionality

```
testsets/
├── jdbc-mysql/              # MySQL JDBC functionality tests
├── jdbc-postgresql/         # PostgreSQL JDBC functionality tests
├── lakehouse-iceberg/       # Iceberg lakehouse functionality tests
├── hive/                    # Hive functionality tests
├── tpch/                    # TPC-H benchmark tests
└── tpcds/                   # TPC-DS benchmark tests
```

#### Test File Naming Convention

Use 5-digit number prefix for easy sorting and reference:
```
00001_basic_query.sql
00002_join_query.sql
00003_aggregate_query.sql
00004_query_pushdown.sql
...
00010_new_feature.sql
```

### 2.8 Common Issues

#### Issue 1: Test output mismatch

**Cause**: Trino version changes causing output format changes

**Solution**: Use wildcard patterns to make expected output more flexible
```
# Before
└─ TableScan[table = gt_mysql:gt_db1.customer, limit=10]

# After
%TableScan[table = gt_mysql:gt_db1.customer%limit%10%]
```

#### Issue 2: Test timeout

**Cause**: Too much test data or complex queries

**Solution**:
- Reduce test data volume
- Simplify queries
- Increase timeout settings

#### Issue 3: Docker container startup failure

**Cause**: Port conflicts or insufficient resources

**Solution**:
```bash
# Cleanup old containers
./integration-test-common/docker-script/shutdown.sh

# Check port usage
lsof -i :8080
lsof -i :9083
```

## Summary

### Running Tests
- Use `trino_integration_test.sh` script to run tests
- Core tool class is `TrinoQueryTestTool`
- Supports multiple test modes and flexible configuration

### Adding/Modifying Tests
1. Create `.sql` file to define test SQL
2. Use `--gen_output` to generate expected output
3. Use wildcard `%` to make expected output flexible
4. Keep tests independent and data volume appropriate
