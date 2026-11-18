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

import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that a version module's binding mistakes fail at build time rather than at use time: a
 * missing binding, a duplicate one, or a blank one.
 */
public class TestSparkBindings {

  @Test
  void testAMissingCatalogIsRejected() {
    SparkBindings.Builder builder =
        SparkBindings.builder()
            .authorizationExtension("org.example.AuthorizationExtensions")
            .catalog(SparkCatalogKind.HIVE, "org.example.HiveCatalog")
            .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, "org.example.IcebergCatalog")
            .catalog(SparkCatalogKind.GLUE, "org.example.GlueCatalog")
            .catalog(SparkCatalogKind.JDBC, "org.example.JdbcCatalog");

    IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, builder::build);
    Assertions.assertTrue(
        e.getMessage().contains(SparkCatalogKind.JDBC_POSTGRESQL.name()), e.getMessage());
  }

  @Test
  void testAMissingAuthorizationExtensionIsRejected() {
    SparkBindings.Builder builder = everyRequiredCatalog(SparkBindings.builder());

    Assertions.assertThrows(IllegalStateException.class, builder::build);
  }

  /**
   * Paimon is the one kind a build may legitimately omit, since Paimon publishes no artifact for
   * every supported Spark and Scala version.
   */
  @Test
  void testPaimonMayBeOmitted() {
    SparkBindings bindings =
        everyRequiredCatalog(SparkBindings.builder())
            .authorizationExtension("org.example.AuthorizationExtensions")
            .build();

    Assertions.assertFalse(
        bindings.catalogClassNames().containsKey(SparkCatalogKind.LAKEHOUSE_PAIMON));
    Assertions.assertEquals(5, bindings.catalogClassNames().size());
  }

  /**
   * Binding the same thing twice is a copy-paste error a version module can make, and the survivor
   * would be whichever line came last. Rejecting it matters most for the authorization extension,
   * where silently keeping the wrong one means sessions run unauthorized.
   */
  @Test
  void testBindingTheSameKindTwiceIsRejected() {
    SparkBindings.Builder builder =
        SparkBindings.builder().catalog(SparkCatalogKind.HIVE, "org.example.HiveCatalog");

    IllegalStateException e =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> builder.catalog(SparkCatalogKind.HIVE, "org.example.OtherHiveCatalog"));

    Assertions.assertTrue(e.getMessage().contains(SparkCatalogKind.HIVE.name()), e.getMessage());
    // The rejected binding must not have replaced the first one.
    SparkBindings bindings =
        builder
            .authorizationExtension("org.example.AuthorizationExtensions")
            .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, "org.example.IcebergCatalog")
            .catalog(SparkCatalogKind.GLUE, "org.example.GlueCatalog")
            .catalog(SparkCatalogKind.JDBC, "org.example.JdbcCatalog")
            .catalog(SparkCatalogKind.JDBC_POSTGRESQL, "org.example.PostgreSqlCatalog")
            .build();
    Assertions.assertEquals(
        "org.example.HiveCatalog", bindings.catalogClassNames().get(SparkCatalogKind.HIVE));
  }

  @Test
  void testBindingTheAuthorizationExtensionTwiceIsRejected() {
    SparkBindings.Builder builder =
        SparkBindings.builder().authorizationExtension("org.example.AuthorizationExtensions");

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> builder.authorizationExtension("org.example.OtherExtensions"));

    // The rejected binding must not have replaced the first one.
    Assertions.assertEquals(
        "org.example.AuthorizationExtensions",
        everyRequiredCatalog(builder).build().authorizationExtension());
  }

  @Test
  void testABlankBindingIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SparkBindings.builder().catalog(SparkCatalogKind.HIVE, " "));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SparkBindings.builder().authorizationExtension(""));
  }

  private static SparkBindings.Builder everyRequiredCatalog(SparkBindings.Builder builder) {
    return builder
        .catalog(SparkCatalogKind.HIVE, "org.example.HiveCatalog")
        .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, "org.example.IcebergCatalog")
        .catalog(SparkCatalogKind.GLUE, "org.example.GlueCatalog")
        .catalog(SparkCatalogKind.JDBC, "org.example.JdbcCatalog")
        .catalog(SparkCatalogKind.JDBC_POSTGRESQL, "org.example.PostgreSqlCatalog");
  }
}
