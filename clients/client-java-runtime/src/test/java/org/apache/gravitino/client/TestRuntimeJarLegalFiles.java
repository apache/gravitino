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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class TestRuntimeJarLegalFiles {
  @Test
  void testEachArtifactHasOneAccurateProjectLicenseAndNotice() throws IOException {
    String license = Files.readString(Paths.get(requiredProperty("legalTemplates"), "LICENSE"));
    String notice = Files.readString(Paths.get(requiredProperty("legalTemplates"), "NOTICE"));
    for (String artifact : requiredProperty("artifacts").split(",")) {
      try (JarFile jar = artifact(artifact)) {
        for (String name : Arrays.asList("LICENSE", "NOTICE")) {
          String path = "META-INF/" + name;
          assertEquals(1L, jar.stream().filter(entry -> path.equals(entry.getName())).count());
          String content = readEntry(jar, path);
          assertTrue(content.startsWith(name.equals("LICENSE") ? license : notice));
          assertFalse(content.contains("web/"), "Unbundled Web UI in " + jar.getName());
          assertFalse(content.contains("DataSketches"), "Server inventory in " + jar.getName());
        }
        long legalEntries =
            jar.stream().filter(entry -> entry.getName().startsWith("META-INF/licenses/")).count();
        long distinctLegalEntries =
            jar.stream()
                .map(JarEntry::getName)
                .filter(name -> name.startsWith("META-INF/licenses/"))
                .distinct()
                .count();
        assertEquals(
            legalEntries,
            distinctLegalEntries,
            "Duplicate dependency legal entries in " + jar.getName());
      }
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testThinAndSourceArtifactsRetainOnlyTheirCopiedSourceNotices() throws IOException {
    for (String name : Arrays.asList("api", "sources")) {
      try (JarFile jar = artifact(name)) {
        String notice = readEntry(jar, "META-INF/NOTICE");
        assertTrue(notice.contains("Apache Spark"));
        assertTrue(notice.contains("Apache Iceberg"));
        assertFalse(notice.contains("Apache Hadoop"));
        assertFalse(
            jar.stream().anyMatch(entry -> entry.getName().startsWith("META-INF/licenses/")));
      }
    }
  }

  @Test
  void testDependencyLegalFilesAndJacksonCompanionsArePreserved() throws IOException {
    int checked = 0;
    try (JarFile jar = artifact("runtime")) {
      for (String path :
          requiredProperty("dependencyJars").split(Pattern.quote(File.pathSeparator))) {
        try (JarFile dependency = new JarFile(path)) {
          String prefix = requiredProperty("dependencyPrefix." + new File(path).getName());
          for (JarEntry entry : Collections.list(dependency.entries())) {
            String name = entry.getName();
            if (!entry.isDirectory()
                && (name.startsWith("META-INF/")
                    && (name.contains("LICENSE") || name.contains("NOTICE")))) {
              assertArrayEquals(
                  readBytes(dependency, name), readBytes(jar, prefix + name), path + ": " + name);
              checked++;
            }
          }
        }
      }
      assertTrue(checked > 10, "Expected distinct dependency documents and Jackson companions");
      assertTrue(readEntry(jar, "META-INF/LICENSE").contains("CC-BY-2.5"));
      assertTrue(readEntry(jar, "META-INF/LICENSE").contains("http://www.jcip.net"));
      assertTrue(
          jar.stream().anyMatch(entry -> entry.getName().endsWith("/FastDoubleParser-LICENSE")));
      assertTrue(
          jar.stream().anyMatch(entry -> entry.getName().endsWith("/LICENSE.jsr305-concurrent")));
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testJavadocKeepsGeneratedAssetLicenses() throws IOException {
    try (JarFile jar = artifact("javadoc")) {
      assertTrue(readEntry(jar, "META-INF/LICENSE").contains("legal/ directory"));
      assertTrue(readEntry(jar, "legal/jquery.md").contains("Permission is hereby granted"));
      assertTrue(readEntry(jar, "legal/jqueryUI.md").contains("Permission is hereby granted"));
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testNestedRuntimeAttributionMatchesBundledSlf4j() throws IOException {
    try (JarFile jar = artifact("filesystem")) {
      assertTrue(readEntry(jar, "META-INF/LICENSE").contains("CC-BY-2.5"));
      assertEquals(
          jar.getJarEntry("org/slf4j/LoggerFactory.class") != null,
          jar.stream()
              .anyMatch(entry -> entry.getName().startsWith("META-INF/licenses/org.slf4j/")));
      assertTrue(
          jar.stream().anyMatch(entry -> entry.getName().contains("/com.fasterxml.jackson.core/")));
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testCaseSensitiveDependencyLicensePathsArePreserved() throws IOException {
    try (JarFile jar = artifact("icebergGcp");
        JarFile dependency = new JarFile(requiredProperty("icebergGcpDependency"))) {
      String prefix =
          jar.stream()
              .map(JarEntry::getName)
              .filter(name -> name.contains("/org.apache.iceberg/iceberg-gcp-bundle/"))
              .filter(name -> name.endsWith("/META-INF/LICENSE"))
              .findFirst()
              .orElseThrow()
              .replace("/META-INF/LICENSE", "/META-INF/");
      for (String name : Arrays.asList("LICENSE", "license/LICENSE.boringssl.txt")) {
        assertArrayEquals(readBytes(dependency, "META-INF/" + name), readBytes(jar, prefix + name));
      }
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testCloudBundlesUseJsseWithoutTheOptionalLgplProvider() throws Exception {
    for (String name : Arrays.asList("aws", "azure")) {
      try (JarFile jar = artifact(name)) {
        assertFalse(
            jar.stream().anyMatch(entry -> entry.getName().contains("org/wildfly/openssl/")));
        assertFalse(
            jar.stream().anyMatch(entry -> entry.getName().contains("/org.wildfly.openssl/")));
      }
      URL[] jars = {
        new File(requiredProperty("artifact." + name)).toURI().toURL(),
        new File(requiredProperty("artifact.runtime")).toURI().toURL()
      };
      // An isolated loader exercises the actual shaded Hadoop implementation and its dependencies.
      try (URLClassLoader loader = new URLClassLoader(jars, ClassLoader.getPlatformClassLoader())) {
        Class<?> factory =
            loader.loadClass("org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory");
        Class<?> mode = loader.loadClass(factory.getName() + "$SSLChannelMode");
        Object defaultMode = mode.getField("Default").get(null);
        factory.getMethod("initializeDefaultFactory", mode).invoke(null, defaultMode);
        Object instance = factory.getMethod("getDefaultFactory").invoke(null);
        assertEquals(
            "Default_JSSE", factory.getMethod("getChannelMode").invoke(instance).toString());
      }
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testAllCloudBundlesExcludeWildFly() throws IOException {
    for (String name : requiredProperty("cloudArtifacts").split(",")) {
      try (JarFile jar = artifact(name)) {
        assertFalse(
            jar.stream()
                .anyMatch(
                    entry ->
                        entry.getName().contains("org/wildfly/openssl/")
                            || entry.getName().contains("/org.wildfly.openssl/")),
            jar.getName());
      }
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testWebWarsRetainTheirOwnLegalDocuments() throws IOException {
    for (String web : Arrays.asList("web", "web-v2")) {
      try (JarFile war = new JarFile(requiredProperty("war." + web))) {
        for (String name : Arrays.asList("LICENSE", "NOTICE")) {
          String path = "META-INF/" + name;
          assertEquals(1L, war.stream().filter(entry -> path.equals(entry.getName())).count());
          assertArrayEquals(
              Files.readAllBytes(Paths.get(requiredProperty("webLegal." + web), name + ".bin")),
              readBytes(war, path),
              web + ": " + name);
        }
      }
    }
  }

  @Test
  void testMainLicenseIdentifiesBundledComponentsAndDocumentPaths() throws IOException {
    for (String name : requiredProperty("artifacts").split(",")) {
      try (JarFile jar = artifact(name)) {
        String license = readEntry(jar, "META-INF/LICENSE");
        for (JarEntry entry : Collections.list(jar.entries())) {
          if (!entry.isDirectory() && entry.getName().startsWith("META-INF/licenses/")) {
            String[] parts = entry.getName().split("/");
            assertTrue(
                license.contains(parts[2] + ":" + parts[3] + ":" + parts[4]), entry.getName());
            assertTrue(license.contains("  " + entry.getName() + "\n"), entry.getName());
          }
        }
      }
    }
  }

  @Test
  void testDependencyNoticesAppearInMainNotice() throws IOException {
    try (JarFile jar = artifact("runtime")) {
      String notice = readEntry(jar, "META-INF/NOTICE");
      for (JarEntry entry : Collections.list(jar.entries())) {
        String path = entry.getName();
        if (path.contains("/com.fasterxml.jackson.core/") && path.endsWith("/META-INF/NOTICE")) {
          assertTrue(notice.contains(readEntry(jar, path).trim()));
          assertTrue(notice.contains(path));
        }
      }
      assertEquals(1, notice.split("Bundled component notices:", -1).length - 1);
    }
  }

  @Test
  @Tag("maven-legal-audit")
  void testNestedNoticeDoesNotRepeatGeneratedInventories() throws IOException {
    try (JarFile jar = artifact("filesystem")) {
      String notice = readEntry(jar, "META-INF/NOTICE");
      assertEquals(1, notice.split("Bundled component notices:", -1).length - 1);
      boolean includesSlf4j = jar.getJarEntry("org/slf4j/LoggerFactory.class") != null;
      assertEquals(
          includesSlf4j, readEntry(jar, "META-INF/LICENSE").contains("org.slf4j:slf4j-api:"));
    }
    try (JarFile jar = artifact("azure")) {
      String license = readEntry(jar, "META-INF/LICENSE");
      assertTrue(license.contains("Microsoft Azure SDK (MIT)"));
      assertTrue(license.contains("Reactive Streams (MIT-0)"));
    }
    try (JarFile jar = artifact("aliyun")) {
      assertFalse(
          jar.stream()
              .anyMatch(
                  entry ->
                      entry.getName().contains("/org.jacoco/")
                          && entry.getName().endsWith("/LICENSE.asm")));
      assertTrue(
          jar.stream()
              .anyMatch(
                  entry ->
                      entry.getName().contains("/org.jacoco/")
                          && entry.getName().endsWith("/about.html")));
    }
  }

  @Test
  void testOlderJacksonIncludesMissingParserLicenseTexts() throws IOException {
    for (String name : requiredProperty("artifacts").split(",")) {
      try (JarFile jar = artifact(name)) {
        for (JarEntry entry : Collections.list(jar.entries())) {
          String path = entry.getName();
          if (path.contains("/com.fasterxml.jackson.core/jackson-core/2.15.2/")
              && path.endsWith("/META-INF/FastDoubleParser-NOTICE")) {
            String prefix = path.substring(0, path.indexOf("/META-INF/FastDoubleParser-NOTICE"));
            assertTrue(
                readEntry(jar, prefix + "/LICENSE.fastdoubleparser-0.9.0")
                    .contains("Copyright (c) 2023 Werner"));
            assertTrue(
                readEntry(jar, prefix + "/LICENSE.schubfach")
                    .contains("Copyright 2018-2020 Raffaello Giulietti"));
            assertTrue(readEntry(jar, "META-INF/LICENSE").contains(prefix + "/LICENSE.schubfach"));
          }
        }
      }
    }
  }

  private static JarFile artifact(String name) throws IOException {
    return new JarFile(requiredProperty("artifact." + name));
  }

  private static String requiredProperty(String name) {
    String value = System.getProperty(name);
    assertNotNull(value, name + " must be provided by the build");
    return value;
  }

  private static String readEntry(JarFile jar, String name) throws IOException {
    return new String(readBytes(jar, name), StandardCharsets.UTF_8);
  }

  private static byte[] readBytes(JarFile jar, String name) throws IOException {
    JarEntry entry = jar.getJarEntry(name);
    assertNotNull(entry, "Missing " + name + " in " + jar.getName());
    try (InputStream input = jar.getInputStream(entry)) {
      return input.readAllBytes();
    }
  }
}
