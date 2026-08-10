/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.junit.jupiter.api.Test;

public class TestIcebergExpireSnapshotsJob {

  @Test
  public void testJobTemplateHasCorrectName() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template);
    assertEquals("builtin-iceberg-expire-snapshots", template.name());
  }

  @Test
  public void testJobTemplateHasComment() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.comment());
    assertFalse(template.comment().trim().isEmpty());
    assertTrue(template.comment().contains("Iceberg"));
  }

  @Test
  public void testJobTemplateHasExecutable() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.executable());
    assertFalse(template.executable().trim().isEmpty());
  }

  @Test
  public void testJobTemplateHasMainClass() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.className());
    assertEquals(IcebergExpireSnapshotsJob.class.getName(), template.className());
  }

  @Test
  public void testJobTemplateHasArguments() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.arguments());
    assertEquals(11, template.arguments().size());

    // Verify all expected arguments are present
    assertTrue(template.arguments().contains("--catalog"));
    assertTrue(template.arguments().contains("{{catalog_name}}"));
    assertTrue(template.arguments().contains("--table"));
    assertTrue(template.arguments().contains("{{table_identifier}}"));
    assertTrue(template.arguments().contains("--older-than"));
    assertTrue(template.arguments().contains("{{older_than}}"));
    assertTrue(template.arguments().contains("--retain-last"));
    assertTrue(template.arguments().contains("{{retain_last}}"));
    // --stream-results is a boolean flag, value is the template variable itself
    assertTrue(template.arguments().contains("{{stream_results}}"));
    assertTrue(template.arguments().contains("--spark-conf"));
    assertTrue(template.arguments().contains("{{spark_conf}}"));
  }

  @Test
  public void testJobTemplateHasSparkConfigs() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    Map<String, String> configs = template.configs();
    assertNotNull(configs);
    assertFalse(configs.isEmpty());

    // Verify Spark runtime configs
    assertTrue(configs.containsKey("spark.master"));
    assertTrue(configs.containsKey("spark.executor.instances"));
    assertTrue(configs.containsKey("spark.executor.cores"));
    assertTrue(configs.containsKey("spark.executor.memory"));
    assertTrue(configs.containsKey("spark.driver.memory"));

    // Verify Iceberg catalog configs
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}.type"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}.uri"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}.warehouse"));

    // Verify Iceberg extensions
    assertTrue(configs.containsKey("spark.sql.extensions"));
    assertEquals(
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        configs.get("spark.sql.extensions"));
  }

  @Test
  public void testJobTemplateHasVersion() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    Map<String, String> customFields = template.customFields();
    assertNotNull(customFields);
    assertTrue(customFields.containsKey(JobTemplateProvider.PROPERTY_VERSION_KEY));

    String version = customFields.get(JobTemplateProvider.PROPERTY_VERSION_KEY);
    assertEquals("v1", version);
    assertTrue(version.matches(JobTemplateProvider.VERSION_VALUE_PATTERN));
  }

  @Test
  public void testJobTemplateNameMatchesBuiltInPattern() {
    IcebergExpireSnapshotsJob job = new IcebergExpireSnapshotsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertTrue(template.name().matches(JobTemplateProvider.BUILTIN_NAME_PATTERN));
    assertTrue(template.name().startsWith(JobTemplateProvider.BUILTIN_NAME_PREFIX));
  }

  // Test parseArguments method

  @Test
  public void testParseArgumentsWithAllRequired() {
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample"};
    Map<String, String> result = IcebergJobUtils.parseArguments(args);

    assertEquals(2, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
  }

  @Test
  public void testParseArgumentsWithOptional() {
    String[] args = {
      "--catalog", "iceberg_prod",
      "--table", "db.sample",
      "--older-than", "2024-01-01 00:00:00",
      "--retain-last", "5"
    };
    Map<String, String> result = IcebergJobUtils.parseArguments(args);

    assertEquals(4, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertEquals("2024-01-01 00:00:00", result.get("older-than"));
    assertEquals("5", result.get("retain-last"));
  }

  @Test
  public void testParseArgumentsWithEmptyValues() {
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample", "--older-than", ""};
    Map<String, String> result = IcebergJobUtils.parseArguments(args);

    // Empty values should be ignored
    assertEquals(2, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertFalse(result.containsKey("older-than"));
  }

  @Test
  public void testParseArgumentsFlagOnly() {
    // --stream-results as a flag (no value) should be treated as "true"
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample", "--stream-results"};
    Map<String, String> result = IcebergJobUtils.parseArguments(args);

    assertEquals(3, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertEquals("true", result.get("stream-results"));
  }

  @Test
  public void testParseArgumentsWithAllOptions() {
    String[] args = {
      "--catalog",
      "iceberg_prod",
      "--table",
      "db.sample",
      "--older-than",
      "2024-06-01 00:00:00",
      "--retain-last",
      "3",
      "--stream-results",
      "--spark-conf",
      "{\"spark.executor.memory\":\"4g\"}"
    };
    Map<String, String> result = IcebergJobUtils.parseArguments(args);

    assertEquals(6, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertEquals("2024-06-01 00:00:00", result.get("older-than"));
    assertEquals("3", result.get("retain-last"));
    assertEquals("true", result.get("stream-results"));
    assertEquals("{\"spark.executor.memory\":\"4g\"}", result.get("spark-conf"));
  }

  @Test
  public void testParseArgumentsOrderIndependent() {
    String[] args1 = {"--catalog", "cat1", "--table", "tbl1", "--retain-last", "5"};
    String[] args2 = {"--retain-last", "5", "--table", "tbl1", "--catalog", "cat1"};

    Map<String, String> result1 = IcebergJobUtils.parseArguments(args1);
    Map<String, String> result2 = IcebergJobUtils.parseArguments(args2);

    assertEquals(result1, result2);
  }

  // Test buildProcedureCall method

  @Test
  public void testBuildProcedureCallMinimal() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, false);

    assertEquals("CALL `iceberg_prod`.system.expire_snapshots(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithOlderThan() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "2024-01-01 00:00:00", null, false);

    assertEquals(
        "CALL `iceberg_prod`.system.expire_snapshots(table => 'db.sample', "
            + "older_than => TIMESTAMP '2024-01-01 00:00:00')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithRetainLast() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall("iceberg_prod", "db.sample", null, "5", false);

    assertEquals(
        "CALL `iceberg_prod`.system.expire_snapshots(table => 'db.sample', retain_last => 5)", sql);
  }

  @Test
  public void testBuildProcedureCallWithStreamResults() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall("iceberg_prod", "db.sample", null, null, true);

    assertEquals(
        "CALL `iceberg_prod`.system.expire_snapshots(table => 'db.sample', "
            + "stream_results => true)",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithAllParameters() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "2024-01-01 00:00:00", "3", true);

    assertTrue(sql.startsWith("CALL `iceberg_prod`.system.expire_snapshots("));
    assertTrue(sql.contains("table => 'db.sample'"));
    assertTrue(sql.contains("older_than => TIMESTAMP '2024-01-01 00:00:00'"));
    assertTrue(sql.contains("retain_last => 3"));
    assertTrue(sql.contains("stream_results => true"));
    assertTrue(sql.endsWith(")"));
  }

  @Test
  public void testBuildProcedureCallWithEmptyOlderThan() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall("iceberg_prod", "db.sample", "", null, false);

    assertEquals("CALL `iceberg_prod`.system.expire_snapshots(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithEmptyRetainLast() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall("iceberg_prod", "db.sample", null, "", false);

    assertEquals("CALL `iceberg_prod`.system.expire_snapshots(table => 'db.sample')", sql);
  }

  // Test SQL escaping

  @Test
  public void testEscapeSqlString() {
    // Test basic escaping of single quotes
    assertEquals("O''Brien", IcebergJobUtils.escapeSqlString("O'Brien"));
    assertEquals("test''with''quotes", IcebergJobUtils.escapeSqlString("test'with'quotes"));

    // Test strings without quotes remain unchanged
    assertEquals("normal_string", IcebergJobUtils.escapeSqlString("normal_string"));

    // Test null and empty
    assertEquals(null, IcebergJobUtils.escapeSqlString(null));
    assertEquals("", IcebergJobUtils.escapeSqlString(""));
  }

  @Test
  public void testEscapeSqlIdentifier() {
    // Test basic escaping and quoting of backticks
    assertEquals("`catalog``name`", IcebergJobUtils.escapeSqlIdentifier("catalog`name"));

    // Test strings without backticks are still quoted
    assertEquals("`normal_catalog`", IcebergJobUtils.escapeSqlIdentifier("normal_catalog"));

    // Test null
    assertEquals(null, IcebergJobUtils.escapeSqlIdentifier(null));
  }

  @Test
  public void testBuildProcedureCallWithSqlInjectionAttempt() {
    // Test SQL injection attempt in table name
    String maliciousTable = "db.table' OR '1'='1";
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            "iceberg_catalog", maliciousTable, null, null, false);

    // Verify single quotes are escaped (becomes '')
    assertTrue(sql.contains("db.table'' OR ''1''=''1"));
    assertFalse(sql.contains("' OR '1'='1"));

    // Test SQL injection attempt in older-than
    String maliciousOlderThan = "2024-01-01' OR '1'='1";
    sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            "iceberg_catalog", "db.table", maliciousOlderThan, null, false);

    assertTrue(sql.contains("2024-01-01'' OR ''1''=''1"));

    // Test SQL injection attempt in catalog name
    String maliciousCatalog = "catalog`; DROP TABLE users; --";
    sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            maliciousCatalog, "db.table", null, null, false);

    // Verify catalog identifier is quoted and backticks are escaped
    assertTrue(sql.contains("`catalog``; DROP TABLE users; --`.system.expire_snapshots"));
  }

  @Test
  public void testBuildProcedureCallEscapesTableIdentifier() {
    String sql =
        IcebergExpireSnapshotsJob.buildProcedureCall(
            "cat'alog", "db'.table", "2024-01-01' DROP TABLE", null, false);

    // Catalog name should be quoted as an identifier
    assertTrue(sql.contains("`cat'alog`"));
    // All single quotes in string literals should be escaped
    assertTrue(sql.contains("db''.table"));
    assertTrue(sql.contains("2024-01-01'' DROP TABLE"));
  }

  // Tests for validateRetainLast

  @Test
  public void testValidateRetainLastWithValidValue() {
    // Should not throw exception
    IcebergExpireSnapshotsJob.validateRetainLast("1");
    IcebergExpireSnapshotsJob.validateRetainLast("5");
    IcebergExpireSnapshotsJob.validateRetainLast("100");
  }

  @Test
  public void testValidateRetainLastWithNull() {
    // Should not throw exception - retain-last is optional
    IcebergExpireSnapshotsJob.validateRetainLast(null);
  }

  @Test
  public void testValidateRetainLastWithEmptyString() {
    // Should not throw exception - retain-last is optional
    IcebergExpireSnapshotsJob.validateRetainLast("");
  }

  @Test
  public void testValidateRetainLastWithZero() {
    try {
      IcebergExpireSnapshotsJob.validateRetainLast("0");
      fail("Expected IllegalArgumentException for zero retain-last");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid retain-last value '0'"));
      assertTrue(e.getMessage().contains("positive integer"));
    }
  }

  @Test
  public void testValidateRetainLastWithNegative() {
    try {
      IcebergExpireSnapshotsJob.validateRetainLast("-1");
      fail("Expected IllegalArgumentException for negative retain-last");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid retain-last value '-1'"));
    }
  }

  @Test
  public void testValidateRetainLastWithNonNumeric() {
    try {
      IcebergExpireSnapshotsJob.validateRetainLast("abc");
      fail("Expected IllegalArgumentException for non-numeric retain-last");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid retain-last value 'abc'"));
      assertTrue(e.getMessage().contains("positive integer"));
    }
  }

  // Tests for custom Spark configurations

  @Test
  public void testParseCustomSparkConfigsWithValidJson() {
    String json = "{\"spark.sql.shuffle.partitions\":\"200\",\"spark.executor.memory\":\"4g\"}";
    Map<String, String> configs = IcebergJobUtils.parseCustomSparkConfigs(json);

    assertEquals(2, configs.size());
    assertEquals("200", configs.get("spark.sql.shuffle.partitions"));
    assertEquals("4g", configs.get("spark.executor.memory"));
  }

  @Test
  public void testParseCustomSparkConfigsWithNumericValues() {
    String json = "{\"spark.sql.shuffle.partitions\":200,\"spark.executor.cores\":4}";
    Map<String, String> configs = IcebergJobUtils.parseCustomSparkConfigs(json);

    assertEquals(2, configs.size());
    assertEquals("200", configs.get("spark.sql.shuffle.partitions"));
    assertEquals("4", configs.get("spark.executor.cores"));
  }

  @Test
  public void testParseCustomSparkConfigsWithEmptyString() {
    Map<String, String> configs = IcebergJobUtils.parseCustomSparkConfigs("");
    assertTrue(configs.isEmpty());
  }

  @Test
  public void testParseCustomSparkConfigsWithNull() {
    Map<String, String> configs = IcebergJobUtils.parseCustomSparkConfigs(null);
    assertTrue(configs.isEmpty());
  }

  @Test
  public void testParseCustomSparkConfigsWithInvalidJson() {
    try {
      IcebergJobUtils.parseCustomSparkConfigs("{invalid json}");
      fail("Expected IllegalArgumentException for invalid JSON");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Failed to parse Spark configurations JSON"));
    }
  }
}
