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

package org.apache.gravitino.maintenance.optimizer.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestIcebergSparkConfigUtils {

  @Test
  public void testBuildTemplateSparkConfigsContainsRequiredKeys() {
    Map<String, String> configs = IcebergSparkConfigUtils.buildTemplateSparkConfigs();

    assertTrue(configs.containsKey("spark.master"));
    assertTrue(configs.containsKey("spark.executor.instances"));
    assertTrue(configs.containsKey("spark.executor.cores"));
    assertTrue(configs.containsKey("spark.executor.memory"));
    assertTrue(configs.containsKey("spark.driver.memory"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}.type"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}.uri"));
    assertTrue(configs.containsKey("spark.sql.catalog.{{catalog_name}}.warehouse"));
    assertEquals("{{catalog_name}}", configs.get("spark.sql.defaultCatalog"));
    assertEquals(
        IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS, configs.get("spark.sql.extensions"));
  }

  @Test
  public void testEnsureIcebergCatalogConfigsSetsDefaultCatalogFromCatalogName() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.sql.catalog.ice_prod.type", "rest");
    configs.put("spark.sql.catalog.ice_prod.uri", "http://localhost:9001/iceberg");

    IcebergSparkConfigUtils.ensureIcebergCatalogConfigs(configs, "ice_prod");

    assertEquals(
        "org.apache.iceberg.spark.SparkCatalog", configs.get("spark.sql.catalog.ice_prod"));
    assertEquals("ice_prod", configs.get("spark.sql.defaultCatalog"));
    assertEquals(
        IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS, configs.get("spark.sql.extensions"));
  }

  @Test
  public void testEnsureIcebergCatalogConfigsRejectsUserDefaultCatalog() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.sql.catalog.ice_prod.type", "rest");
    configs.put("spark.sql.defaultCatalog", "spark_catalog");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergSparkConfigUtils.ensureIcebergCatalogConfigs(configs, "ice_prod"));
    assertTrue(e.getMessage().contains("spark.sql.defaultCatalog"));
    assertTrue(e.getMessage().contains("catalog_name"));
  }

  @Test
  public void testBuildTemplateSparkConfigsLeavesMasterUnsetByDefault() {
    Map<String, String> configs = IcebergSparkConfigUtils.buildTemplateSparkConfigs();

    assertEquals("{{spark_master:-}}", configs.get("spark.master"));
  }

  @Test
  public void testResolveAndOverlayFlatWinsWithoutInjectingDefaults() {
    Map<String, String> spark = new HashMap<>();
    spark.put(IcebergSparkConfigUtils.SPARK_EXECUTOR_MEMORY_KEY, "4g");
    spark.put(IcebergSparkConfigUtils.SPARK_EXECUTOR_CORES_KEY, "2");
    Map<String, String> flat = new HashMap<>();
    flat.put(IcebergSparkConfigUtils.PLACEHOLDER_SPARK_EXECUTOR_MEMORY, "7g");
    flat.put(IcebergSparkConfigUtils.PLACEHOLDER_CATALOG_TYPE, "rest");

    Map<String, String> placeholders =
        IcebergSparkConfigUtils.resolveTemplateSparkPlaceholders("iceberg", spark, flat);
    assertEquals("7g", placeholders.get(IcebergSparkConfigUtils.PLACEHOLDER_SPARK_EXECUTOR_MEMORY));
    assertEquals("rest", placeholders.get(IcebergSparkConfigUtils.PLACEHOLDER_CATALOG_TYPE));

    IcebergSparkConfigUtils.overlayFlatResourceAndCatalogKeys(spark, flat, "iceberg");
    assertEquals("7g", spark.get(IcebergSparkConfigUtils.SPARK_EXECUTOR_MEMORY_KEY));
    assertEquals("2", spark.get(IcebergSparkConfigUtils.SPARK_EXECUTOR_CORES_KEY));
    assertEquals("rest", spark.get("spark.sql.catalog.iceberg.type"));
    assertTrue(!spark.containsKey(IcebergSparkConfigUtils.SPARK_EXECUTOR_INSTANCES_KEY));
  }

  @Test
  public void testParseFlatJsonMap() {
    Map<String, String> parsed =
        IcebergSparkConfigUtils.parseFlatJsonMap(
            "{\"k1\":\"v1\",\"k2\":1,\"k3\":null}", "spark-conf");

    assertEquals("v1", parsed.get("k1"));
    assertEquals("1", parsed.get("k2"));
    assertEquals("", parsed.get("k3"));
  }

  @Test
  public void testParseFlatJsonMapRejectsNestedValue() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergSparkConfigUtils.parseFlatJsonMap(
                    "{\"spark\":{\"master\":\"local[2]\"}}", "spark-conf"));
    assertTrue(e.getMessage().contains("flat key-value JSON map"));
  }

  @Test
  public void testValidateSparkConfigsForRestCatalog() {
    Map<String, String> sparkConfigs = baseSparkConfigs("rest");
    sparkConfigs.put("spark.sql.catalog.rest.uri", "http://localhost:9001/iceberg");
    sparkConfigs.put("spark.sql.catalog.rest.warehouse", "/tmp/warehouse");

    IcebergSparkConfigUtils.validateSparkConfigsForCatalog(sparkConfigs, "rest", "rest.ab.t1");
  }

  @Test
  public void testValidateSparkConfigsForHadoopCatalog() {
    Map<String, String> sparkConfigs = baseSparkConfigs("hadoop");
    sparkConfigs.put("spark.sql.catalog.rest.warehouse", "/tmp/warehouse");

    IcebergSparkConfigUtils.validateSparkConfigsForCatalog(sparkConfigs, "rest", "rest.ab.t1");
  }

  @Test
  public void testValidateSparkConfigsAllowsMissingExtensions() {
    Map<String, String> sparkConfigs = baseSparkConfigs("rest");
    sparkConfigs.remove("spark.sql.extensions");
    sparkConfigs.put("spark.sql.catalog.rest.uri", "http://localhost:9001/iceberg");
    IcebergSparkConfigUtils.validateSparkConfigsForCatalog(sparkConfigs, "rest", "rest.ab.t1");
  }

  @Test
  public void testValidateSparkConfigsMissingUriForRestCatalog() {
    Map<String, String> sparkConfigs = baseSparkConfigs("rest");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergSparkConfigUtils.validateSparkConfigsForCatalog(
                    sparkConfigs, "rest", "rest.ab.t1"));
    assertTrue(e.getMessage().contains("spark.sql.catalog.rest.uri"));
  }

  private Map<String, String> baseSparkConfigs(String catalogType) {
    Map<String, String> configs = new HashMap<>();
    configs.put(
        "spark.sql.extensions",
        "com.example.Ext," + IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS);
    configs.put("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog");
    configs.put("spark.sql.catalog.rest.type", catalogType);
    return configs;
  }
}
