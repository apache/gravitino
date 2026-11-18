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
package org.apache.gravitino.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TestRuntimeJarDependencies {
  private static final String FACTORY_SERVICE_ENTRY =
      "META-INF/services/org.apache.flink.table.factories.Factory";

  @Test
  void shadowJarShouldNotBundleSlf4j() throws IOException {
    String jarPath = System.getProperty("shadowJarPath");
    assertNotNull(jarPath, "shadowJarPath system property should be provided by the build");

    File runtimeJar = new File(jarPath);
    assertTrue(runtimeJar.exists(), "shadow jar does not exist: " + runtimeJar);

    try (JarFile jarFile = new JarFile(runtimeJar)) {
      List<String> entries = jarFile.stream().map(JarEntry::getName).collect(Collectors.toList());
      boolean hasSlf4jClasses = entries.stream().anyMatch(entry -> entry.startsWith("org/slf4j/"));
      boolean hasSlf4jMetadata =
          entries.stream().anyMatch(entry -> entry.startsWith("META-INF/maven/org.slf4j/"));
      assertFalse(
          hasSlf4jClasses,
          "Flink connector runtime jar should rely on Flink provided slf4j instead of shading it");
      assertFalse(hasSlf4jMetadata, "SLF4J metadata should not be packaged in runtime jar");
    }
  }

  @Test
  void shadowJarShouldMergeFactoryServiceDescriptors() throws IOException {
    String jarPath = System.getProperty("shadowJarPath");
    assertNotNull(jarPath, "shadowJarPath system property should be provided by the build");

    try (JarFile jarFile = new JarFile(new File(jarPath))) {
      JarEntry factoryServiceEntry = jarFile.getJarEntry(FACTORY_SERVICE_ENTRY);
      assertNotNull(factoryServiceEntry, "Factory service descriptor should exist in runtime jar");

      String factoryServices;
      try (InputStream in = jarFile.getInputStream(factoryServiceEntry)) {
        factoryServices = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      List<String> gravitinoFactories =
          factoryServices
              .lines()
              .map(line -> line.split("#", 2)[0].trim())
              .filter(line -> line.startsWith("org.apache.gravitino.flink."))
              .collect(Collectors.toList());
      Set<String> expectedFactories =
          Set.of(
              "org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactory",
              "org.apache.gravitino.flink.connector.paimon.GravitinoPaimonCatalogFactoryFlink22",
              "org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryFlink22",
              "org.apache.gravitino.flink.connector.jdbc.mysql.GravitinoMysqlJdbcCatalogFactoryFlink22",
              "org.apache.gravitino.flink.connector.jdbc.postgresql.GravitinoPostgresJdbcCatalogFactoryFlink22");
      assertEquals(
          expectedFactories,
          new HashSet<>(gravitinoFactories),
          "Runtime jar should register exactly the shared catalog-store and four Flink 2.2 factories");
      assertEquals(
          expectedFactories.size(),
          gravitinoFactories.size(),
          "Runtime jar should not contain duplicate Gravitino factory registrations");
      for (String factory : expectedFactories) {
        assertNotNull(
            jarFile.getJarEntry(factory.replace('.', '/') + ".class"),
            "Registered factory class should exist in runtime jar: " + factory);
      }
    }
  }
}
