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

package org.apache.gravitino.utils;

import java.lang.reflect.Method;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIsolatedClassLoader {

  private IsolatedClassLoader classLoader;
  private Method isCatalogClassMethod;

  @BeforeEach
  public void setUp() throws Exception {
    classLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    isCatalogClassMethod =
        IsolatedClassLoader.class.getDeclaredMethod("isCatalogClass", String.class);
    isCatalogClassMethod.setAccessible(true);
  }

  private boolean isCatalogClass(String name) throws Exception {
    return (boolean) isCatalogClassMethod.invoke(classLoader, name);
  }

  @Test
  public void testHivePackageRecognizedAsCatalogClass() throws Exception {
    // org.apache.gravitino.hive.* was moved from catalog.hive.* by HiveClient refactoring.
    // These must be treated as catalog classes so they are loaded by the IsolatedClassLoader,
    // not the server classloader. Otherwise their compiler-generated $1 synthetic classes
    // (from switch-on-enum) fail to load and the JVM permanently caches the failure.
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.hive.client.HiveExceptionConverter"));
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.hive.client.HiveExceptionConverter$1"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.hive.client.HiveClientPool"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.hive.HiveClientFactory"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.hive.SomeOtherClass"));
  }

  @Test
  public void testCatalogHivePackageRecognizedAsCatalogClass() throws Exception {
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.hive.HiveCatalogCapability"));
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.hive.HiveCatalogCapability$1"));
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.hive.HiveCatalogOperations"));
  }

  @Test
  public void testOtherCatalogPackagesRecognizedAsCatalogClass() throws Exception {
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.jdbc.JdbcCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.kafka.KafkaCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.fileset.FilesetCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.model.ModelCatalog"));
  }

  @Test
  public void testNonCatalogPackagesNotRecognizedAsCatalogClass() throws Exception {
    // Server-side / shared classes must NOT be treated as catalog classes.
    Assertions.assertFalse(isCatalogClass("org.apache.gravitino.connector.BaseCatalog"));
    Assertions.assertFalse(isCatalogClass("org.apache.gravitino.NameIdentifier"));
    Assertions.assertFalse(isCatalogClass("org.apache.gravitino.catalog.SomeSharedClass"));
    Assertions.assertFalse(isCatalogClass("java.lang.String"));
    Assertions.assertFalse(isCatalogClass("org.slf4j.Logger"));
  }
}
