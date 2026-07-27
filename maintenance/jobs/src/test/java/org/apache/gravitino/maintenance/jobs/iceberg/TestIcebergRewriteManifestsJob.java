/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

public class TestIcebergRewriteManifestsJob {

  @Test
  public void testJobTemplateHasCorrectName() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template);
    assertEquals("builtin-iceberg-rewrite-manifests", template.name());
  }

  @Test
  public void testJobTemplateHasComment() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.comment());
    assertFalse(template.comment().trim().isEmpty());
    assertTrue(template.comment().contains("Iceberg"));
    assertTrue(template.comment().contains("manifests"));
  }

  @Test
  public void testJobTemplateHasExecutable() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.executable());
    assertFalse(template.executable().trim().isEmpty());
  }

  @Test
  public void testJobTemplateHasMainClass() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.className());
    assertEquals(IcebergRewriteManifestsJob.class.getName(), template.className());
  }

  @Test
  public void testJobTemplateHasArguments() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.arguments());
    assertEquals(8, template.arguments().size()); // 4 flags * 2 (flag + value)

    // Verify all expected arguments are present
    assertTrue(template.arguments().contains("--catalog"));
    assertTrue(template.arguments().contains("{{catalog_name}}"));
    assertTrue(template.arguments().contains("--table"));
    assertTrue(template.arguments().contains("{{table_identifier}}"));
    assertTrue(template.arguments().contains("--use-caching"));
    assertTrue(template.arguments().contains("{{use_caching}}"));
    assertTrue(template.arguments().contains("--spark-conf"));
    assertTrue(template.arguments().contains("{{spark_conf}}"));
  }

  @Test
  public void testJobTemplateHasSparkConfigs() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
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
  }

  @Test
  public void testJobTemplateHasVersion() {
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
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
    IcebergRewriteManifestsJob job = new IcebergRewriteManifestsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertTrue(template.name().matches(JobTemplateProvider.BUILTIN_NAME_PATTERN));
    assertTrue(template.name().startsWith(JobTemplateProvider.BUILTIN_NAME_PREFIX));
  }

  // Test parseArguments method

  @Test
  public void testParseArgumentsWithAllRequired() {
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample"};
    Map<String, String> result = IcebergRewriteManifestsJob.parseArguments(args);

    assertEquals(2, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
  }

  @Test
  public void testParseArgumentsWithOptional() {
    String[] args = {
      "--catalog", "iceberg_prod",
      "--table", "db.sample",
      "--use-caching", "false"
    };
    Map<String, String> result = IcebergRewriteManifestsJob.parseArguments(args);

    assertEquals(3, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertEquals("false", result.get("use-caching"));
  }

  @Test
  public void testParseArgumentsWithEmptyValues() {
    String[] args = {"--catalog", "iceberg_prod", "--table", "db.sample", "--use-caching", ""};
    Map<String, String> result = IcebergRewriteManifestsJob.parseArguments(args);

    // Empty values should be ignored
    assertEquals(2, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertEquals("db.sample", result.get("table"));
    assertFalse(result.containsKey("use-caching"));
  }

  @Test
  public void testParseArgumentsWithMissingValues() {
    String[] args = {"--catalog", "iceberg_prod", "--table"};
    Map<String, String> result = IcebergRewriteManifestsJob.parseArguments(args);

    // Only catalog should be parsed, table has no value
    assertEquals(1, result.size());
    assertEquals("iceberg_prod", result.get("catalog"));
    assertFalse(result.containsKey("table"));
  }

  @Test
  public void testParseArgumentsOrderIndependent() {
    String[] args1 = {"--catalog", "cat1", "--table", "tbl1", "--use-caching", "true"};
    String[] args2 = {"--use-caching", "true", "--table", "tbl1", "--catalog", "cat1"};

    Map<String, String> result1 = IcebergRewriteManifestsJob.parseArguments(args1);
    Map<String, String> result2 = IcebergRewriteManifestsJob.parseArguments(args2);

    assertEquals(result1, result2);
  }

  // Test buildProcedureCall method

  @Test
  public void testBuildProcedureCallMinimal() {
    String sql =
        IcebergRewriteManifestsJob.buildProcedureCall("iceberg_prod", "db.sample", null);

    assertEquals("CALL iceberg_prod.system.rewrite_manifests(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithCachingTrue() {
    String sql =
        IcebergRewriteManifestsJob.buildProcedureCall("iceberg_prod", "db.sample", "true");

    assertEquals(
        "CALL iceberg_prod.system.rewrite_manifests(table => 'db.sample', use_caching => true)",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithCachingFalse() {
    String sql =
        IcebergRewriteManifestsJob.buildProcedureCall("iceberg_prod", "db.sample", "false");

    assertEquals(
        "CALL iceberg_prod.system.rewrite_manifests(table => 'db.sample', use_caching => false)",
        sql);
  }

  @Test
  public void testBuildProcedureCallWithEmptyCaching() {
    String sql =
        IcebergRewriteManifestsJob.buildProcedureCall("iceberg_prod", "db.sample", "");

    assertEquals("CALL iceberg_prod.system.rewrite_manifests(table => 'db.sample')", sql);
  }

  @Test
  public void testBuildProcedureCallWithSqlInjectionAttempt() {
    // Test SQL injection attempt in table name
    String maliciousTable = "db.table' OR '1'='1";
    String sql =
        IcebergRewriteManifestsJob.buildProcedureCall("iceberg_catalog", maliciousTable, null);

    // Verify single quotes are escaped (becomes '')
    assertTrue(sql.contains("db.table'' OR ''1''=''1"));
    assertFalse(sql.contains("' OR '1'='1"));

    // Test SQL injection attempt in catalog name
    String maliciousCatalog = "catalog`; DROP TABLE users; --";
    sql =
        IcebergRewriteManifestsJob.buildProcedureCall(maliciousCatalog, "db.table", null);

    // Verify backticks are escaped
    assertTrue(sql.contains("catalog``; DROP TABLE users; --"));
  }
}