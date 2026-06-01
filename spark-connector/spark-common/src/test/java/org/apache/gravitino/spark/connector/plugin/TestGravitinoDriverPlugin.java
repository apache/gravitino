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

package org.apache.gravitino.spark.connector.plugin;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoDriverPlugin {

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }

  @Test
  void testEngineAccessMode() {
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.AUTO,
        GravitinoDriverPlugin.EngineAccessMode.from(null));
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.AUTO,
        GravitinoDriverPlugin.EngineAccessMode.from(""));
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.AUTO,
        GravitinoDriverPlugin.EngineAccessMode.from(" auto "));
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.GRAVITINO,
        GravitinoDriverPlugin.EngineAccessMode.from("gravitino"));
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.NATIVE,
        GravitinoDriverPlugin.EngineAccessMode.from("NATIVE"));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoDriverPlugin.EngineAccessMode.from("unsupported"));
    Assertions.assertTrue(exception.getMessage().contains("auto, gravitino, native"));
  }

  @Test
  void testGetProviderEngineAccessMode() {
    SparkConf conf = new SparkConf();
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.AUTO,
        GravitinoDriverPlugin.getEngineAccessMode(conf, "lakehouse-iceberg"));

    conf.set(GravitinoSparkConfig.engineAccessModeConfig("lakehouse-iceberg"), "native");
    Assertions.assertEquals(
        GravitinoDriverPlugin.EngineAccessMode.NATIVE,
        GravitinoDriverPlugin.getEngineAccessMode(conf, "lakehouse-iceberg"));
  }

  @Test
  void testGetIcebergExtensions() {
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin();
    SparkConf conf = new SparkConf();
    Assertions.assertEquals(
        Arrays.asList(
            GravitinoIcebergSparkSessionExtensions.class.getName(),
            GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS),
        plugin.getIcebergExtensions(conf));

    conf.set(GravitinoSparkConfig.engineAccessModeConfig("lakehouse-iceberg"), "native");
    Assertions.assertEquals(
        Collections.singletonList(GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS),
        plugin.getIcebergExtensions(conf));
  }

  @Test
  void testBuildNativeIcebergCatalogConfigurations() {
    Map<String, String> configs =
        GravitinoDriverPlugin.buildNativeIcebergCatalogConfigurations(
            "iceberg_catalog",
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "REST",
                IcebergConstants.URI,
                "http://localhost:8181",
                IcebergConstants.WAREHOUSE,
                "s3://warehouse",
                IcebergConstants.DATA_ACCESS,
                "vended-credentials"));

    String catalogConfigPrefix = "spark.sql.catalog.iceberg_catalog";
    Assertions.assertEquals(
        GravitinoDriverPlugin.ICEBERG_SPARK_CATALOG, configs.get(catalogConfigPrefix));
    Assertions.assertEquals(
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
        configs.get(catalogConfigPrefix + "." + CatalogUtil.ICEBERG_CATALOG_TYPE));
    Assertions.assertEquals(
        "http://localhost:8181", configs.get(catalogConfigPrefix + "." + CatalogProperties.URI));
    Assertions.assertEquals(
        "s3://warehouse",
        configs.get(catalogConfigPrefix + "." + CatalogProperties.WAREHOUSE_LOCATION));
    Assertions.assertEquals(
        GravitinoDriverPlugin.VENDED_CREDENTIALS,
        configs.get(
            catalogConfigPrefix + "." + GravitinoDriverPlugin.ICEBERG_ACCESS_DELEGATION_HEADER));
  }

  @Test
  void testBuildNativeIcebergCatalogConfigurationsWithoutOptionalProperties() {
    Map<String, String> configs =
        GravitinoDriverPlugin.buildNativeIcebergCatalogConfigurations(
            "iceberg_catalog",
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "rest",
                IcebergConstants.URI,
                "http://localhost:8181"));

    String catalogConfigPrefix = "spark.sql.catalog.iceberg_catalog";
    Assertions.assertFalse(
        configs.containsKey(catalogConfigPrefix + "." + CatalogProperties.WAREHOUSE_LOCATION));
    Assertions.assertFalse(
        configs.containsKey(
            catalogConfigPrefix + "." + GravitinoDriverPlugin.ICEBERG_ACCESS_DELEGATION_HEADER));
  }

  @Test
  void testBuildNativeIcebergCatalogConfigurationsWithInvalidBackend() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoDriverPlugin.buildNativeIcebergCatalogConfigurations(
                    "iceberg_catalog",
                    ImmutableMap.of(
                        IcebergConstants.CATALOG_BACKEND,
                        "hive",
                        IcebergConstants.URI,
                        "thrift://localhost:9083")));
    Assertions.assertTrue(exception.getMessage().contains("catalog-backend=rest"));
  }

  @Test
  void testBuildNativeIcebergCatalogConfigurationsWithMissingUri() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoDriverPlugin.buildNativeIcebergCatalogConfigurations(
                    "iceberg_catalog", ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "rest")));
    Assertions.assertTrue(exception.getMessage().contains("uri should not be empty"));
  }
}
