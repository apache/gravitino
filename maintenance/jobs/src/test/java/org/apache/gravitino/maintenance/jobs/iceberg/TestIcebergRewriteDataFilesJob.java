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
    assertEquals(12, template.arguments().size()); // 6 flags * 2 (flag + value)

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

    assertTrue(sql.contains("where => '" + whereClause + "'"));
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
}
