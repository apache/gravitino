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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.util.Collections;
import java.util.Locale;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class TestRuntimeJarLegalFiles {
  @Test
  void testCanonicalDocumentsAndUniqueLegalEntries() throws IOException {
    try (JarFile jar = artifact()) {
      for (String name : Arrays.asList("LICENSE", "NOTICE")) {
        String path = "META-INF/" + name;
        assertEquals(1L, jar.stream().filter(entry -> path.equals(entry.getName())).count());
        String marker =
            name.equals("LICENSE") ? "Bundled component licensing:" : "Bundled component notices:";
        String projectText = readEntry(jar, path).split(Pattern.quote(marker), 2)[0].trim();
        assertEquals(
            Files.readString(Paths.get(System.getProperty("legalTemplates"), name)).trim(),
            projectText);
      }
      long count =
          jar.stream().filter(entry -> entry.getName().startsWith("META-INF/licenses/")).count();
      assertEquals(
          count,
          jar.stream()
              .map(JarEntry::getName)
              .filter(name -> name.startsWith("META-INF/licenses/"))
              .distinct()
              .count());
    }
  }

  @Test
  void testDependencyLegalDocumentsArePreserved() throws IOException {
    int checked = 0;
    try (JarFile jar = artifact()) {
      for (String path :
          System.getProperty("dependencyJars").split(Pattern.quote(File.pathSeparator))) {
        try (JarFile dependency = new JarFile(path)) {
          String prefix = System.getProperty("dependencyPrefix." + new File(path).getName());
          for (JarEntry entry : Collections.list(dependency.entries())) {
            String name = entry.getName();
            if (!entry.isDirectory()
                && name.startsWith("META-INF/")
                && (name.contains("LICENSE") || name.contains("NOTICE"))) {
              assertArrayEquals(
                  readBytes(dependency, name), readBytes(jar, prefix + name), path + ": " + name);
              checked++;
            }
          }
        }
      }
    }
    assertTrue(checked > 0, "No dependency legal documents were checked");
  }

  @Test
  void testJacksonCoreEmbeddedComponentLicensesAreBundled() throws IOException {
    String prefix = dependencyPrefix("jackson-core-");
    try (JarFile jar = artifact()) {
      // jackson-core embeds FastDoubleParser and Schubfach under MIT terms. Their texts come either
      // from the jackson-core jar itself or from a supplement in dependencies.txt, depending on the
      // Jackson version, so only require that each one is shipped with the jackson-core documents.
      for (String component : Arrays.asList("fastdoubleparser", "schubfach")) {
        assertTrue(
            jar.stream()
                .map(JarEntry::getName)
                .anyMatch(
                    name ->
                        name.startsWith(prefix)
                            && name.substring(prefix.length())
                                .toLowerCase(Locale.ROOT)
                                .contains(component)),
            "Missing the " + component + " license under " + prefix);
      }
    }
  }

  @Test
  void testMainDocumentsIdentifyBundledComponentsAndNotices() throws IOException {
    try (JarFile jar = artifact()) {
      String license = readEntry(jar, "META-INF/LICENSE");
      String notice = readEntry(jar, "META-INF/NOTICE");
      int checkedNotices = 0;
      for (JarEntry entry : Collections.list(jar.entries())) {
        String path = entry.getName();
        if (!entry.isDirectory() && path.startsWith("META-INF/licenses/")) {
          String[] parts = path.split("/");
          assertTrue(license.contains(parts[2] + ":" + parts[3] + ":" + parts[4]), path);
          assertTrue(license.contains("  " + path + "\n"), path);
          if (path.contains("/com.fasterxml.jackson.core/") && path.endsWith("/META-INF/NOTICE")) {
            assertTrue(notice.contains(readEntry(jar, path).trim()), path);
            assertTrue(notice.contains(path));
            checkedNotices++;
          }
        }
      }
      assertTrue(checkedNotices > 0);
      assertEquals(1, notice.split("Bundled component notices:", -1).length - 1);
    }
  }

  private static JarFile artifact() throws IOException {
    return new JarFile(System.getProperty("artifactPath"));
  }

  private static String dependencyPrefix(String jarNamePrefix) {
    for (String path :
        System.getProperty("dependencyJars").split(Pattern.quote(File.pathSeparator))) {
      String name = new File(path).getName();
      if (name.startsWith(jarNamePrefix)) {
        return System.getProperty("dependencyPrefix." + name);
      }
    }
    throw new AssertionError("No bundled dependency jar starts with " + jarNamePrefix);
  }

  private static String readEntry(JarFile jar, String path) throws IOException {
    return new String(readBytes(jar, path), StandardCharsets.UTF_8);
  }

  private static byte[] readBytes(JarFile jar, String path) throws IOException {
    JarEntry entry = jar.getJarEntry(path);
    assertNotNull(entry, "Missing " + path + " in " + jar.getName());
    try (InputStream input = jar.getInputStream(entry)) {
      return input.readAllBytes();
    }
  }
}
