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

import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class TestIcebergRewriteDataFilesJob {

  @Test
  public void testJobTemplateHasCorrectName() {
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template);
    assertEquals("builtin-iceberg-rewrite-data-files", template.name());
  }

  @Test
  public void testJobTemplateHasComment() {
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.comment());
    assertFalse(template.comment().trim().isEmpty());
    assertTrue(template.comment().contains("Iceberg"));
  }

  @Test
  public void testJobTemplateHasExecutable() {
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.executable());
    assertFalse(template.executable().trim().isEmpty());
  }

  @Test
  public void testJobTemplateHasMainClass() {
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.className());
    assertEquals(IcebergRewriteDataFilesJob.class.getName(), template.className());
  }

  @Test
  public void testJobTemplateHasArguments() {
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
    SparkJobTemplate template = job.jobTemplate();

    // The where clause has no default: an empty one rewrites the whole table, so callers must
    // pass it explicitly.
    assertEquals(
        Arrays.asList(
            "--catalog",
            "{{catalog_name}}",
            "--table",
            "{{table_identifier}}",
            "--strategy",
            "{{strategy:-binpack}}",
            "--sort-order",
            "{{sort_order:-}}",
            "--where",
            "{{where_clause}}",
            "--options",
            "{{options:-}}",
            "--spark-conf",
            "{{spark_conf:-}}"),
        template.arguments());
  }

  @Test
  public void testJobTemplateHasSparkConfigs() {
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
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
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
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
    IcebergRewriteDataFilesJob job = new IcebergRewriteDataFilesJob();
    SparkJobTemplate template = job.jobTemplate();

    assertTrue(template.name().matches(JobTemplateProvider.BUILTIN_NAME_PATTERN));
    assertTrue(template.name().startsWith(JobTemplateProvider.BUILTIN_NAME_PREFIX));
  }

  // Test parseOptionsJson method

  @Test
  public void testParseOptionsJsonWithSingleOption() {
    String json = "{\"min-input-files\":\"2\"}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(1, result.size());
    assertEquals("2", result.get("min-input-files"));
  }

  @Test
  public void testParseOptionsJsonWithMultipleOptions() {
    String json = "{\"min-input-files\":\"2\",\"target-file-size-bytes\":\"536870912\"}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(2, result.size());
    assertEquals("2", result.get("min-input-files"));
    assertEquals("536870912", result.get("target-file-size-bytes"));
  }

  @Test
  public void testParseOptionsJsonWithBooleanValues() {
    String json =
        "{\"rewrite-all\":\"true\",\"partial-progress.enabled\":\"true\",\"remove-dangling-deletes\":\"false\"}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(3, result.size());
    assertEquals("true", result.get("rewrite-all"));
    assertEquals("true", result.get("partial-progress.enabled"));
    assertEquals("false", result.get("remove-dangling-deletes"));
  }

  @Test
  public void testParseOptionsJsonWithEmptyString() {
    String json = "";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseOptionsJsonWithNull() {
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(null);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseOptionsJsonWithEmptyObject() {
    String json = "{}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseOptionsJsonWithColonsInValue() {
    // Test handling of colons in values (e.g., file paths)
    String json = "{\"path\":\"/data/file:backup\",\"url\":\"http://example.com:8080\"}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(2, result.size());
    assertEquals("/data/file:backup", result.get("path"));
    assertEquals("http://example.com:8080", result.get("url"));
  }

  @Test
  public void testParseOptionsJsonWithCommasInValue() {
    // Test handling of commas in values
    String json = "{\"list\":\"item1,item2,item3\",\"description\":\"a,b,c\"}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(2, result.size());
    assertEquals("item1,item2,item3", result.get("list"));
    assertEquals("a,b,c", result.get("description"));
  }

  @Test
  public void testParseOptionsJsonWithEscapedQuotes() {
    // Test handling of escaped quotes in values
    String json = "{\"message\":\"He said \\\"hello\\\"\",\"name\":\"O'Brien\"}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(2, result.size());
    assertEquals("He said \"hello\"", result.get("message"));
    assertEquals("O'Brien", result.get("name"));
  }

  @Test
  public void testParseOptionsJsonWithNumericValues() {
    // Test handling of numeric values (should be converted to strings)
    String json = "{\"max-size\":1073741824,\"min-files\":2,\"ratio\":0.75}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(3, result.size());
    assertEquals("1073741824", result.get("max-size"));
    assertEquals("2", result.get("min-files"));
    assertEquals("0.75", result.get("ratio"));
  }

  @Test
  public void testParseOptionsJsonWithBooleanTypes() {
    // Test handling of boolean values (not strings)
    String json = "{\"enabled\":true,\"disabled\":false}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(2, result.size());
    assertEquals("true", result.get("enabled"));
    assertEquals("false", result.get("disabled"));
  }

  @Test
  public void testParseOptionsJsonWithComplexValue() {
    // Test with a realistic complex case
    String json =
        "{\"file-path\":\"/data/path:backup,archive\","
            + "\"description\":\"Rewrite with strategy: binpack, sort\","
            + "\"max-size\":536870912,"
            + "\"enabled\":true}";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(4, result.size());
    assertEquals("/data/path:backup,archive", result.get("file-path"));
    assertEquals("Rewrite with strategy: binpack, sort", result.get("description"));
    assertEquals("536870912", result.get("max-size"));
    assertEquals("true", result.get("enabled"));
  }

  @Test
  public void testParseOptionsJsonWithInvalidJson() {
    // Test that invalid JSON throws an exception
    String json = "{invalid json}";
    try {
      IcebergRewriteDataFilesJob.parseOptionsJson(json);
      fail("Expected IllegalArgumentException for invalid JSON");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("--options"));
    }
  }

  @Test
  public void testParseOptionsJsonWithSpaces() {
    String json = "{ \"min-input-files\" : \"2\" , \"target-file-size-bytes\" : \"1024\" }";
    Map<String, String> result = IcebergRewriteDataFilesJob.parseOptionsJson(json);

    assertEquals(2, result.size());
    assertEquals("2", result.get("min-input-files"));
    assertEquals("1024", result.get("target-file-size-bytes"));
  }

  // Test buildProcedureCall method

  @Test
  public void testBuildProcedureCallMinimal() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, null, null);

    assertEquals("CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithStrategy() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "binpack", null, null, null);

    assertEquals(
        "CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample', strategy => 'binpack')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithSortOrder() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "sort", "id DESC NULLS LAST", null, null);

    assertEquals(
        "CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample', strategy => 'sort', sort_order => 'id DESC NULLS LAST')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithWhere() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, "year = 2024", null);

    assertEquals(
        "CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample', where => 'year = 2024')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithOptions() {
    String optionsJson = "{\"min-input-files\":\"2\",\"target-file-size-bytes\":\"536870912\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, null, optionsJson);

    assertTrue(
        sql.startsWith("CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample'"));
    assertTrue(sql.contains("options => map("));
    assertTrue(sql.contains("'min-input-files', '2'"));
    assertTrue(sql.contains("'target-file-size-bytes', '536870912'"));
    assertTrue(sql.endsWith(")"));
  }

  @Test
  public void testBuildProcedureCallWithAllParameters() {
    String optionsJson = "{\"min-input-files\":\"2\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "sort", "id DESC NULLS LAST", "year = 2024", optionsJson);

    assertTrue(sql.startsWith("CALL `iceberg_prod`.system.rewrite_data_files("));
    assertTrue(sql.contains("table => 'db.sample'"));
    assertTrue(sql.contains("strategy => 'sort'"));
    assertTrue(sql.contains("sort_order => 'id DESC NULLS LAST'"));
    assertTrue(sql.contains("where => 'year = 2024'"));
    assertTrue(sql.contains("options => map('min-input-files', '2')"));
    assertTrue(sql.endsWith(")"));
  }

  @Test
  public void testBuildProcedureCallWithEmptyStrategy() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "", null, null, null);

    assertEquals("CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithEmptyOptions() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, null, "{}");

    assertEquals("CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithZOrder() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "sort", "zorder(c1,c2,c3)", null, null);

    assertEquals(
        "CALL `iceberg_prod`.system.rewrite_data_files(table => 'db.sample', strategy => 'sort', sort_order => 'zorder(c1,c2,c3)')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithComplexWhere() {
    String whereClause = "year = 2024 AND month >= 1 AND month <= 12 AND status = 'active'";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, whereClause, null);

    // Single quotes in the WHERE clause should be escaped
    assertTrue(
        sql.contains(
            "where => 'year = 2024 AND month >= 1 AND month <= 12 AND status = ''active'''"));
  }

  @Test
  public void testBuildProcedureCallWithMultipleOptions() {
    String optionsJson =
        "{\"min-input-files\":\"5\",\"target-file-size-bytes\":\"1073741824\",\"remove-dangling-deletes\":\"true\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "binpack", null, null, optionsJson);

    assertTrue(sql.contains("'min-input-files', '5'"));
    assertTrue(sql.contains("'target-file-size-bytes', '1073741824'"));
    assertTrue(sql.contains("'remove-dangling-deletes', 'true'"));
  }

  @Test
  public void testBuildProcedureCallWithSqlInjectionAttempt() {
    // Test SQL injection attempt in table name
    String maliciousTable = "db.table' OR '1'='1";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_catalog", maliciousTable, null, null, null, null);

    // Verify single quotes are escaped (becomes '')
    assertTrue(sql.contains("db.table'' OR ''1''=''1"));
    assertFalse(sql.contains("' OR '1'='1"));

    // Test SQL injection attempt in where clause
    String maliciousWhere = "year = 2024' OR '1'='1";
    sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_catalog", "db.table", null, null, maliciousWhere, null);

    assertTrue(sql.contains("year = 2024'' OR ''1''=''1"));

    // Test SQL injection attempt in catalog name
    String maliciousCatalog = "catalog`; DROP TABLE users; --";
    sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            maliciousCatalog, "db.table", null, null, null, null);

    // Verify catalog identifier is quoted and backticks are escaped
    assertTrue(sql.contains("`catalog``; DROP TABLE users; --`.system.rewrite_data_files"));
  }

  @Test
  public void testBuildProcedureCallEscapesAllParameters() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "cat'alog", "db'.table", "sort'", "id' DESC", "year' = 2024", "{\"key'\":\"val'ue\"}");

    // Catalog name should be quoted as an identifier
    assertTrue(sql.contains("`cat'alog`"));
    // All single quotes in string literals should be escaped
    assertTrue(sql.contains("db''.table"));
    assertTrue(sql.contains("sort''"));
    assertTrue(sql.contains("id'' DESC"));
    assertTrue(sql.contains("year'' = 2024"));
    assertTrue(sql.contains("key''"));
    assertTrue(sql.contains("val''ue"));
  }

  // Tests for strategy validation

  @Test
  public void testValidateStrategyWithValidBinpack() {
    // Should not throw exception
    IcebergRewriteDataFilesJob.validateStrategy("binpack");
  }

  @Test
  public void testValidateStrategyWithValidSort() {
    // Should not throw exception
    IcebergRewriteDataFilesJob.validateStrategy("sort");
  }

  @Test
  public void testValidateStrategyWithNull() {
    // Should not throw exception - strategy is optional
    IcebergRewriteDataFilesJob.validateStrategy(null);
  }

  @Test
  public void testValidateStrategyWithEmptyString() {
    // Should not throw exception - strategy is optional
    IcebergRewriteDataFilesJob.validateStrategy("");
  }

  @Test
  public void testValidateStrategyWithInvalidValue() {
    try {
      IcebergRewriteDataFilesJob.validateStrategy("invalid");
      fail("Expected IllegalArgumentException for invalid strategy");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid strategy 'invalid'"));
      assertTrue(e.getMessage().contains("'binpack'"));
      assertTrue(e.getMessage().contains("'sort'"));
    }
  }

  @Test
  public void testValidateStrategyWithZorder() {
    // z-order is a sort order, not a strategy
    try {
      IcebergRewriteDataFilesJob.validateStrategy("z-order");
      fail("Expected IllegalArgumentException for z-order as strategy");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid strategy 'z-order'"));
      assertTrue(e.getMessage().contains("Valid values are: 'binpack', 'sort'"));
    }
  }

  @Test
  public void testValidateStrategyIsCaseSensitive() {
    // Strategy validation should be case-sensitive
    try {
      IcebergRewriteDataFilesJob.validateStrategy("BINPACK");
      fail("Expected IllegalArgumentException for uppercase BINPACK");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid strategy 'BINPACK'"));
    }
  }

  @Test
  public void testCompactionMetricsSchemaForTms() {
    Map<String, Object> metrics =
        IcebergRewriteDataFilesJob.aggregatePartitionCompactionMetrics(new Row[0]);
    assertEquals(5, metrics.size());
    assertEquals(
        0D,
        ((Number) metrics.get(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC)).doubleValue());
    assertEquals(
        0L,
        ((Number) metrics.get(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC)).longValue());
    assertEquals(
        0D,
        ((Number) metrics.get(IcebergDataCompactionContent.AVG_FILE_SIZE_METRIC)).doubleValue());
    assertEquals(
        0L,
        ((Number) metrics.get(IcebergDataCompactionContent.TOTAL_FILE_SIZE_METRIC)).longValue());
    assertEquals(
        0L, ((Number) metrics.get(IcebergDataCompactionContent.FILE_NUMBER_METRIC)).longValue());
  }
}
