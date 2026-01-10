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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
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

    assertNotNull(template.arguments());
    assertEquals(14, template.arguments().size()); // 7 flags * 2 (flag + value)

    // Verify all expected arguments are present
    assertTrue(template.arguments().contains("--catalog"));
    assertTrue(template.arguments().contains("{{catalog_name}}"));
    assertTrue(template.arguments().contains("--table"));
    assertTrue(template.arguments().contains("{{table_identifier}}"));
    assertTrue(template.arguments().contains("--strategy"));
    assertTrue(template.arguments().contains("{{strategy}}"));
    assertTrue(template.arguments().contains("--sort-order"));
    assertTrue(template.arguments().contains("{{sort_order}}"));
    assertTrue(template.arguments().contains("--where"));
    assertTrue(template.arguments().contains("{{where_clause}}"));
    assertTrue(template.arguments().contains("--options"));
    assertTrue(template.arguments().contains("{{options}}"));
    assertTrue(template.arguments().contains("--spark-conf"));
    assertTrue(template.arguments().contains("{{spark_conf}}"));
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

  // Test parseArguments method

  @Test
  public void testParseArgumentsWithAllRequired() {
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample"};
    Map<String, String> result = IcebergRewriteDataFilesJob.parseArguments(args);

    assertEquals(2, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
  }

  @Test
  public void testParseArgumentsWithOptional() {
    String[] args = {
      "--catalog", "iceberg_prod",
      "--table", "db.sample",
      "--strategy", "binpack",
      "--where", "year = 2024"
    };
    Map<String, String> result = IcebergRewriteDataFilesJob.parseArguments(args);

    assertEquals(4, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertEquals("binpack", result.get("strategy"));
    assertEquals("year = 2024", result.get("where"));
  }

  @Test
  public void testParseArgumentsWithEmptyValues() {
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample", "--strategy", ""};
    Map<String, String> result = IcebergRewriteDataFilesJob.parseArguments(args);

    // Empty values should be ignored
    assertEquals(2, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertFalse(result.containsKey("strategy"));
  }

  @Test
  public void testParseArgumentsWithMissingValues() {
    String[] args = {"--catalog", "iceberg_prod", "--table"};
    Map<String, String> result = IcebergRewriteDataFilesJob.parseArguments(args);

    // Only catalog should be parsed, table has no value
    assertEquals(1, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertFalse(result.containsKey("table"));
  }

  @Test
  public void testParseArgumentsWithComplexValues() {
    String[] args = {
      "--catalog", "iceberg_prod",
      "--table", "db.sample",
      "--where", "year = 2024 and month = 1",
      "--options", "{\"min-input-files\":\"2\"}"
    };
    Map<String, String> result = IcebergRewriteDataFilesJob.parseArguments(args);

    assertEquals(4, result.size());
    assertEquals("year = 2024 and month = 1", result.get("where"));
    assertEquals("{\"min-input-files\":\"2\"}", result.get("options"));
  }

  @Test
  public void testParseArgumentsWithSortOrder() {
    String[] args = {
      "--catalog", "iceberg_prod",
      "--table", "db.sample",
      "--strategy", "sort",
      "--sort-order", "id DESC NULLS LAST, name ASC"
    };
    Map<String, String> result = IcebergRewriteDataFilesJob.parseArguments(args);

    assertEquals(4, result.size());
    assertEquals("sort", result.get("strategy"));
    assertEquals("id DESC NULLS LAST, name ASC", result.get("sort-order"));
  }

  @Test
  public void testParseArgumentsOrderIndependent() {
    String[] args1 = {"--catalog", "cat1", "--table", "tbl1", "--strategy", "binpack"};
    String[] args2 = {"--strategy", "binpack", "--table", "tbl1", "--catalog", "cat1"};

    Map<String, String> result1 = IcebergRewriteDataFilesJob.parseArguments(args1);
    Map<String, String> result2 = IcebergRewriteDataFilesJob.parseArguments(args2);

    assertEquals(result1, result2);
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
      assertTrue(e.getMessage().contains("Failed to parse options JSON"));
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

    assertEquals("CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithStrategy() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "binpack", null, null, null);

    assertEquals(
        "CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample', strategy => 'binpack')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithSortOrder() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "sort", "id DESC NULLS LAST", null, null);

    assertEquals(
        "CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample', strategy => 'sort', sort_order => 'id DESC NULLS LAST')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithWhere() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, "year = 2024", null);

    assertEquals(
        "CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample', where => 'year = 2024')",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithOptions() {
    String optionsJson = "{\"min-input-files\":\"2\",\"target-file-size-bytes\":\"536870912\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, null, optionsJson);

    assertTrue(sql.startsWith("CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample'"));
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

    assertTrue(sql.startsWith("CALL iceberg_prod.system.rewrite_data_files("));
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

    assertEquals("CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithEmptyOptions() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", null, null, null, "{}");

    assertEquals("CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithZOrder() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "iceberg_prod", "db.sample", "sort", "zorder(c1,c2,c3)", null, null);

    assertEquals(
        "CALL iceberg_prod.system.rewrite_data_files(table => 'db.sample', strategy => 'sort', sort_order => 'zorder(c1,c2,c3)')",
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
  public void testEscapeSqlString() {
    // Test basic escaping of single quotes
    assertEquals("O''Brien", IcebergRewriteDataFilesJob.escapeSqlString("O'Brien"));
    assertEquals(
        "test''with''quotes", IcebergRewriteDataFilesJob.escapeSqlString("test'with'quotes"));

    // Test strings without quotes remain unchanged
    assertEquals("normal_string", IcebergRewriteDataFilesJob.escapeSqlString("normal_string"));

    // Test null and empty
    assertEquals(null, IcebergRewriteDataFilesJob.escapeSqlString(null));
    assertEquals("", IcebergRewriteDataFilesJob.escapeSqlString(""));
  }

  @Test
  public void testEscapeSqlIdentifier() {
    // Test basic escaping of backticks
    assertEquals("catalog``name", IcebergRewriteDataFilesJob.escapeSqlIdentifier("catalog`name"));

    // Test strings without backticks remain unchanged
    assertEquals(
        "normal_catalog", IcebergRewriteDataFilesJob.escapeSqlIdentifier("normal_catalog"));

    // Test null
    assertEquals(null, IcebergRewriteDataFilesJob.escapeSqlIdentifier(null));
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

    // Verify backticks are escaped
    assertTrue(sql.contains("catalog``; DROP TABLE users; --"));
  }

  @Test
  public void testBuildProcedureCallEscapesAllParameters() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            "cat'alog", "db'.table", "sort'", "id' DESC", "year' = 2024", "{\"key'\":\"val'ue\"}");

    // Catalog name uses backtick escaping (but no backticks here, so unchanged)
    assertTrue(sql.contains("cat'alog"));
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

  // Tests for custom Spark configurations

  @Test
  public void testParseCustomSparkConfigsWithValidJson() {
    String json = "{\"spark.sql.shuffle.partitions\":\"200\",\"spark.executor.memory\":\"4g\"}";
    Map<String, String> configs = IcebergRewriteDataFilesJob.parseCustomSparkConfigs(json);

    assertEquals(2, configs.size());
    assertEquals("200", configs.get("spark.sql.shuffle.partitions"));
    assertEquals("4g", configs.get("spark.executor.memory"));
  }

  @Test
  public void testParseCustomSparkConfigsWithNumericValues() {
    String json = "{\"spark.sql.shuffle.partitions\":200,\"spark.executor.cores\":4}";
    Map<String, String> configs = IcebergRewriteDataFilesJob.parseCustomSparkConfigs(json);

    assertEquals(2, configs.size());
    assertEquals("200", configs.get("spark.sql.shuffle.partitions"));
    assertEquals("4", configs.get("spark.executor.cores"));
  }

  @Test
  public void testParseCustomSparkConfigsWithBooleanValues() {
    String json = "{\"spark.dynamicAllocation.enabled\":true,\"spark.speculation\":false}";
    Map<String, String> configs = IcebergRewriteDataFilesJob.parseCustomSparkConfigs(json);

    assertEquals(2, configs.size());
    assertEquals("true", configs.get("spark.dynamicAllocation.enabled"));
    assertEquals("false", configs.get("spark.speculation"));
  }

  @Test
  public void testParseCustomSparkConfigsWithEmptyString() {
    Map<String, String> configs = IcebergRewriteDataFilesJob.parseCustomSparkConfigs("");
    assertTrue(configs.isEmpty());
  }

  @Test
  public void testParseCustomSparkConfigsWithNull() {
    Map<String, String> configs = IcebergRewriteDataFilesJob.parseCustomSparkConfigs(null);
    assertTrue(configs.isEmpty());
  }

  @Test
  public void testParseCustomSparkConfigsWithInvalidJson() {
    String json = "{invalid json}";
    try {
      IcebergRewriteDataFilesJob.parseCustomSparkConfigs(json);
      fail("Expected IllegalArgumentException for invalid JSON");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Failed to parse Spark configurations JSON"));
    }
  }

  @Test
  public void testValidateSparkConfigsWithValidConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.sql.shuffle.partitions", "200");
    configs.put("spark.executor.memory", "4g");
    configs.put("spark.dynamicAllocation.enabled", "true");

    // Should not throw exception
    IcebergRewriteDataFilesJob.validateSparkConfigs(configs);
  }

  @Test
  public void testValidateSparkConfigsRejectsCatalogOverride() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.sql.catalog.my_catalog", "some.catalog.Class");

    try {
      IcebergRewriteDataFilesJob.validateSparkConfigs(configs);
      fail("Expected IllegalArgumentException for catalog config override");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("cannot be overridden"));
      assertTrue(e.getMessage().contains("spark.sql.catalog."));
    }
  }

  @Test
  public void testValidateSparkConfigsRejectsExtensionsOverride() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.sql.extensions", "some.other.Extension");

    try {
      IcebergRewriteDataFilesJob.validateSparkConfigs(configs);
      fail("Expected IllegalArgumentException for extensions config override");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("cannot be overridden"));
      assertTrue(e.getMessage().contains("spark.sql.extensions"));
    }
  }

  @Test
  public void testValidateSparkConfigsRejectsAppNameOverride() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.app.name", "MyCustomApp");

    try {
      IcebergRewriteDataFilesJob.validateSparkConfigs(configs);
      fail("Expected IllegalArgumentException for app name override");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("cannot be overridden"));
      assertTrue(e.getMessage().contains("spark.app.name"));
    }
  }

  @Test
  public void testValidateSparkConfigsRejectsNonSparkPrefix() {
    Map<String, String> configs = new HashMap<>();
    configs.put("hadoop.fs.defaultFS", "hdfs://namenode:9000");

    try {
      IcebergRewriteDataFilesJob.validateSparkConfigs(configs);
      fail("Expected IllegalArgumentException for non-spark config");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must start with 'spark.'"));
      assertTrue(e.getMessage().contains("hadoop.fs.defaultFS"));
    }
  }

  @Test
  public void testValidateSparkConfigsWithEmptyMap() {
    Map<String, String> configs = new HashMap<>();
    // Should not throw exception
    IcebergRewriteDataFilesJob.validateSparkConfigs(configs);
  }
}
