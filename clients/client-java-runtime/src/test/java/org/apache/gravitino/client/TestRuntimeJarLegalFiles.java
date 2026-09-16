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
package org.apache.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class TestRuntimeJarLegalFiles {
  private static final List<String> LEGAL_FILES = Arrays.asList("LICENSE", "NOTICE");

  @Test
  void testLegalFilesHaveSingleEntriesWithProjectContent() throws IOException {
    try (JarFile jar = new JarFile(requiredProperty("shadowJarPath"))) {
      for (String name : LEGAL_FILES) {
        String entryName = "META-INF/" + name;
        assertEquals(
            1L,
            jar.stream().filter(entry -> entryName.equals(entry.getName())).count(),
            "Expected exactly one " + entryName);
        String projectContent =
            new String(
                Files.readAllBytes(Paths.get(requiredProperty("projectLegalFile." + name))),
                StandardCharsets.UTF_8);
        String mergedContent = readEntry(jar, entryName);
        assertTrue(
            mergedContent.startsWith(projectContent),
            "Gravitino content must be first in " + entryName);
        assertEquals(
            mergedContent.indexOf(projectContent),
            mergedContent.lastIndexOf(projectContent),
            "Duplicate Gravitino content in " + entryName);
      }
    }
  }

  @Test
  void testDependencyLegalContentIsPreserved() throws IOException {
    int checkedEntries = 0;
    try (JarFile jar = new JarFile(requiredProperty("shadowJarPath"))) {
      for (String path :
          requiredProperty("dependencyJars").split(Pattern.quote(File.pathSeparator))) {
        try (JarFile dependency = new JarFile(path)) {
          for (String name : LEGAL_FILES) {
            String entryName = "META-INF/" + name;
            if (dependency.getJarEntry(entryName) != null) {
              assertTrue(
                  readEntry(jar, entryName).contains(readEntry(dependency, entryName)),
                  "Missing " + entryName + " content from " + path);
              checkedEntries++;
            }
          }
        }
      }
    }
    assertTrue(checkedEntries > 0, "Expected dependency legal files to be checked");
  }

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    assertNotNull(value, name + " must be provided by the build");
    return value;
  }

  private static String readEntry(JarFile jar, String name) throws IOException {
    JarEntry entry = jar.getJarEntry(name);
    assertNotNull(entry, "Missing " + name + " in " + jar.getName());
    try (InputStream input = jar.getInputStream(entry)) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
