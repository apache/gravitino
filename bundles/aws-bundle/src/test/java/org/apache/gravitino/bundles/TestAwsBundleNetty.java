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
package org.apache.gravitino.bundles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

class TestAwsBundleNetty {

  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));
  private static final String NETTY_VERSION = System.getProperty("nettyVersion");
  private static final String NETTY_METADATA = "META-INF/maven/io.netty/";

  @Test
  void testNettyIsRelocated() throws Exception {
    try (JarFile jar = new JarFile(BUNDLE)) {
      assertFalse(
          jar.stream().anyMatch(entry -> entry.getName().startsWith("io/netty/")),
          BUNDLE.getName());
    }
  }

  @Test
  void testBundleContainsSingleNettyRelease() throws Exception {
    assertNotNull(NETTY_VERSION);
    Set<String> versions = new HashSet<>();
    try (JarFile jar = new JarFile(BUNDLE)) {
      for (JarEntry entry : (Iterable<JarEntry>) jar.stream()::iterator) {
        String name = entry.getName();
        // netty-tcnative has its own version scheme.
        if (name.startsWith(NETTY_METADATA)
            && name.endsWith("/pom.properties")
            && !name.startsWith(NETTY_METADATA + "netty-tcnative")) {
          Properties properties = new Properties();
          try (InputStream in = jar.getInputStream(entry)) {
            properties.load(in);
          }
          versions.add(properties.getProperty("version"));
        }
      }
    }
    assertEquals(Set.of(NETTY_VERSION), versions, BUNDLE.getName());
  }
}
