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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TestRuntimeJarDependencies {

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
}
