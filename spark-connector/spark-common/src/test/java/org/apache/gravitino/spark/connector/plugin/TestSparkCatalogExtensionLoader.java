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

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that discovered {@link SparkCatalogExtension}s are merged into a {@link
 * SparkBindings.Builder}, or skipped, correctly. Registration runs through {@link
 * SparkBindings.Builder#build()} in these tests, the same path a real build exercises, since {@link
 * SparkBindings.Builder}'s catalog map is private.
 */
public class TestSparkCatalogExtensionLoader {

  @Test
  void testADiscoveredExtensionFillsAnOmittedKind() {
    SparkBindings bindings =
        buildWithExtensions(
            List.of(fixedExtension("lakehouse-paimon", "org.example.PaimonCatalog")));

    Assertions.assertEquals(
        "org.example.PaimonCatalog",
        bindings.catalogClassNames().get(SparkCatalogKind.LAKEHOUSE_PAIMON));
  }

  @Test
  void testAnExtensionForAnUnknownProviderIsSkipped() {
    SparkBindings bindings =
        buildWithExtensions(List.of(fixedExtension("no-such-provider", "org.example.Catalog")));

    Assertions.assertFalse(
        bindings.catalogClassNames().containsKey(SparkCatalogKind.LAKEHOUSE_PAIMON));
    Assertions.assertEquals(5, bindings.catalogClassNames().size());
  }

  @Test
  void testAnExtensionForAnAlreadyBoundKindIsSkipped() {
    SparkBindings bindings =
        buildWithExtensions(List.of(fixedExtension("hive", "org.example.OtherHiveCatalog")));

    Assertions.assertEquals(
        "org.example.HiveCatalog", bindings.catalogClassNames().get(SparkCatalogKind.HIVE));
  }

  @Test
  void testAnExtensionThatThrowsIsSkippedRatherThanFailingTheWholeLoad() {
    SparkBindings bindings =
        buildWithExtensions(
            List.of(
                throwingExtension(),
                fixedExtension("lakehouse-paimon", "org.example.PaimonCatalog")));

    Assertions.assertEquals(
        "org.example.PaimonCatalog",
        bindings.catalogClassNames().get(SparkCatalogKind.LAKEHOUSE_PAIMON));
  }

  @Test
  void testNoExtensionsIsANoOp() {
    SparkBindings bindings = buildWithExtensions(Collections.emptyList());

    Assertions.assertFalse(
        bindings.catalogClassNames().containsKey(SparkCatalogKind.LAKEHOUSE_PAIMON));
    Assertions.assertEquals(5, bindings.catalogClassNames().size());
  }

  private static SparkBindings buildWithExtensions(List<SparkCatalogExtension> extensions) {
    SparkBindings.Builder builder =
        SparkBindings.builder()
            .authorizationExtension("org.example.AuthorizationExtensions")
            .catalog(SparkCatalogKind.HIVE, "org.example.HiveCatalog")
            .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, "org.example.IcebergCatalog")
            .catalog(SparkCatalogKind.GLUE, "org.example.GlueCatalog")
            .catalog(SparkCatalogKind.JDBC, "org.example.JdbcCatalog")
            .catalog(SparkCatalogKind.JDBC_POSTGRESQL, "org.example.PostgreSqlCatalog");
    SparkCatalogExtensionLoader.registerDiscoveredCatalogs(builder, extensions.iterator());
    return builder.build();
  }

  private static SparkCatalogExtension fixedExtension(String provider, String catalogClassName) {
    return new SparkCatalogExtension() {
      @Override
      public String provider() {
        return provider;
      }

      @Override
      public String catalogClassName() {
        return catalogClassName;
      }
    };
  }

  private static SparkCatalogExtension throwingExtension() {
    return new SparkCatalogExtension() {
      @Override
      public String provider() {
        throw new IllegalStateException("boom");
      }

      @Override
      public String catalogClassName() {
        throw new IllegalStateException("boom");
      }
    };
  }
}
