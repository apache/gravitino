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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests what the plugin does with the bindings it is given. The bindings here are made up, so these
 * assertions hold on every Spark version and do not restate what the version modules declare.
 */
public class TestGravitinoDriverPlugin {

  private static final String AUTHZ_EXTENSION = "org.example.AuthorizationExtensions";
  private static final String PAIMON_CATALOG = "org.example.PaimonCatalog";

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }

  @Test
  void testAlwaysRegistersTheBoundAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);

    new GravitinoDriverPlugin(withoutPaimon()).registerSqlExtensions(sparkConf);

    assertEquals(AUTHZ_EXTENSION, sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testDoesNotDuplicateAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), AUTHZ_EXTENSION);

    new GravitinoDriverPlugin(withoutPaimon()).registerSqlExtensions(sparkConf);

    assertEquals(AUTHZ_EXTENSION, sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  /**
   * Paimon publishes no paimon-spark artifact for every Spark version and Scala version this
   * connector supports, so some builds bind no Paimon catalog. Registering the Paimon session
   * extension there would fail SparkSession construction, so the plugin must skip it whenever the
   * binding is absent.
   */
  @Test
  void testPaimonExtensionIsSkippedWithoutAPaimonBinding() {
    assertFalse(registeredExtensions(withoutPaimon()).contains(paimonExtension()));
  }

  @Test
  void testPaimonExtensionIsRegisteredWithAPaimonBinding() {
    assertTrue(registeredExtensions(withPaimon()).contains(paimonExtension()));
  }

  private static String registeredExtensions(SparkBindings bindings) {
    SparkConf sparkConf = new SparkConf(false);
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(bindings);
    plugin.registerPaimonExtensionsIfSupported();
    plugin.registerSqlExtensions(sparkConf);
    return sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
  }

  private static String paimonExtension() {
    return GravitinoDriverPlugin.PAIMON_SPARK_EXTENSIONS;
  }

  private static SparkBindings withoutPaimon() {
    return requiredCatalogs().build();
  }

  private static SparkBindings withPaimon() {
    return requiredCatalogs().catalog(SparkCatalogKind.LAKEHOUSE_PAIMON, PAIMON_CATALOG).build();
  }

  private static SparkBindings.Builder requiredCatalogs() {
    return SparkBindings.builder()
        .authorizationExtension(AUTHZ_EXTENSION)
        .catalog(SparkCatalogKind.HIVE, "org.example.HiveCatalog")
        .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, "org.example.IcebergCatalog")
        .catalog(SparkCatalogKind.GLUE, "org.example.GlueCatalog")
        .catalog(SparkCatalogKind.JDBC, "org.example.JdbcCatalog")
        .catalog(SparkCatalogKind.JDBC_POSTGRESQL, "org.example.PostgreSqlCatalog");
  }
}
