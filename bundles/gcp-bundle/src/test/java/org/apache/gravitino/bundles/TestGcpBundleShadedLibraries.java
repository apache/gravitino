/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.bundles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

/**
 * Checks the libraries that hadoop-client-runtime shades into the gcp bundle. Hadoop 3.3 shaded
 * Avro 1.7.7 and protobuf 3.7.1, Hadoop 3.4.3 ships Avro 1.11.4 and protobuf 3.25.
 */
class TestGcpBundleShadedLibraries {

  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));

  @Test
  void testAvroIsNotOutdated() throws Exception {
    for (String version : versionsOf("META-INF/maven/org.apache.avro/avro/pom.properties")) {
      assertTrue(compare(version, "1.11.4") >= 0, "Outdated Avro " + version);
    }
  }

  @Test
  void testProtobufIsNotOutdated() throws Exception {
    assertTrue(
        versionsOf(
                "META-INF/maven/org.apache.hadoop.thirdparty/hadoop-shaded-protobuf_3_7/pom.properties")
            .isEmpty(),
        "hadoop-shaded-protobuf_3_7 is outdated");
    for (String version :
        versionsOf("META-INF/maven/com.google.protobuf/protobuf-java/pom.properties")) {
      assertTrue(compare(version, "3.25.0") >= 0, "Outdated protobuf " + version);
    }
    try (JarFile jar = new JarFile(BUNDLE)) {
      assertFalse(
          jar.stream()
              .anyMatch(
                  e -> e.getName().startsWith("org/apache/hadoop/shaded/com/google/protobuf/")),
          "Protobuf 3.7 is shaded under org/apache/hadoop/shaded");
    }
  }

  private static List<String> versionsOf(String propertiesEntry) throws Exception {
    List<String> versions = new ArrayList<>();
    try (JarFile jar = new JarFile(BUNDLE)) {
      for (JarEntry entry : (Iterable<JarEntry>) jar.stream()::iterator) {
        if (entry.getName().equals(propertiesEntry)) {
          Properties properties = new Properties();
          try (InputStream in = jar.getInputStream(entry)) {
            properties.load(in);
          }
          versions.add(properties.getProperty("version"));
        }
      }
    }
    return versions;
  }

  private static int compare(String version, String minimum) {
    String[] left = version.split("[.-]");
    String[] right = minimum.split("[.-]");
    for (int i = 0; i < right.length; i++) {
      int l = i < left.length ? Integer.parseInt(left[i]) : 0;
      int r = Integer.parseInt(right[i]);
      if (l != r) {
        return Integer.compare(l, r);
      }
    }
    return 0;
  }
}
