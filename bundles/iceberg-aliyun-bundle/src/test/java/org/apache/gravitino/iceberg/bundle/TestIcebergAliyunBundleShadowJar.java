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
package org.apache.gravitino.iceberg.bundle;

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

class TestIcebergAliyunBundleShadowJar {
  private static final String RELOCATED_JACKSON_PREFIX =
      "org/apache/gravitino/iceberg/aliyun/shaded/com/fasterxml/jackson/";

  @Test
  void jacksonShouldBeRelocated() throws IOException {
    String jarPath = System.getProperty("shadowJarPath");
    assertNotNull(jarPath, "shadowJarPath system property should be provided by the build");

    File shadowJar = new File(jarPath);
    assertTrue(shadowJar.exists(), "shadow jar does not exist: " + shadowJar);

    try (JarFile jarFile = new JarFile(shadowJar)) {
      List<String> entries = jarFile.stream().map(JarEntry::getName).collect(Collectors.toList());

      assertTrue(
          entries.stream().anyMatch(entry -> entry.startsWith(RELOCATED_JACKSON_PREFIX)),
          "Iceberg Aliyun bundle should keep Jackson under a relocated namespace");
      assertFalse(
          entries.stream().anyMatch(TestIcebergAliyunBundleShadowJar::isUnshadedJacksonEntry),
          "Iceberg Aliyun bundle should not expose unshaded Jackson classes or services");
    }
  }

  private static boolean isUnshadedJacksonEntry(String entry) {
    if (entry.endsWith("/")) {
      return false;
    }
    return entry.startsWith("com/fasterxml/jackson/")
        || entry.matches("META-INF/versions/[^/]+/com/fasterxml/jackson/.*")
        || entry.startsWith("META-INF/maven/com.fasterxml.jackson")
        || entry.startsWith("META-INF/services/com.fasterxml.jackson");
  }
}
