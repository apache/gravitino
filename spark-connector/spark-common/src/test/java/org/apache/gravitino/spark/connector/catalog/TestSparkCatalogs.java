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
package org.apache.gravitino.spark.connector.catalog;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests the catalog table each version module declares. Compiles into every version module, so it
 * asserts the properties that must hold on every Spark version rather than a fixed set of classes;
 * the class names themselves are already checked by the compiler.
 */
public class TestSparkCatalogs {

  @Test
  void testEveryDeclaredCatalogClassIsOnTheClasspath() throws ClassNotFoundException {
    Map<SparkCatalogKind, String> classNames = SparkCatalogs.classNames();
    Assertions.assertFalse(classNames.isEmpty(), "A connector build must declare some catalog");
    for (Map.Entry<SparkCatalogKind, String> entry : classNames.entrySet()) {
      Class<?> catalogClass = Class.forName(entry.getValue());
      Assertions.assertTrue(
          BaseCatalog.class.isAssignableFrom(catalogClass),
          entry.getValue() + " must be a BaseCatalog");
    }
  }

  @Test
  void testTheCatalogsEveryVersionMustShipArePresent() {
    // Paimon is deliberately absent from some builds, so it is not in this list. Every other kind
    // has to be there, or a Gravitino catalog that used to work would silently stop registering.
    for (SparkCatalogKind kind :
        new SparkCatalogKind[] {
          SparkCatalogKind.HIVE,
          SparkCatalogKind.LAKEHOUSE_ICEBERG,
          SparkCatalogKind.GLUE,
          SparkCatalogKind.JDBC,
          SparkCatalogKind.JDBC_POSTGRESQL
        }) {
      Assertions.assertNotNull(
          SparkCatalogs.classNames().get(kind), kind + " must have a catalog in every build");
    }
  }
}
