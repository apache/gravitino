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
package org.apache.gravitino.lance.common.ops.hive;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Guards the runtime classpath against the legacy Jetty artifacts that the Hive metastore client
 * (hive-common 2.3.9, pulled transitively by lance-namespace-hive2) drags in.
 *
 * <p>Those jars — {@code org.eclipse.jetty.aggregate:jetty-all:7.6.0} (where {@code
 * org.eclipse.jetty.util.component.Container} is a <em>class</em>), {@code org.mortbay.jetty:*},
 * and {@code org.eclipse.jetty.orbit:*} — collide with Gravitino's Jetty 9.4.x (where {@code
 * Container} is an <em>interface</em>). In the packaged distribution the jars in {@code libs/} are
 * loaded in alphabetical order, so {@code jetty-all-7.6.0} shadows the modern Jetty and the
 * standalone server dies at startup with {@code IncompatibleClassChangeError: class
 * org.eclipse.jetty.server.Connector can not implement ...Container}.
 *
 * <p>A functional integration test does not reliably catch this: Gradle's test classpath is ordered
 * by the dependency graph (modern Jetty first), so the good class loads and the server starts. This
 * test instead inspects the classpath directly, so it fails deterministically regardless of load
 * order.
 */
class TestHiveClasspathHygiene {

  /** Jar-name fragments that must never appear on the runtime classpath. */
  private static final List<String> FORBIDDEN_JAR_FRAGMENTS = Arrays.asList("jetty-all", "mortbay");

  @Test
  void testNoLegacyJettyOnClasspath() {
    String classpath = System.getProperty("java.class.path", "");
    List<String> offenders =
        Arrays.stream(classpath.split(java.io.File.pathSeparator))
            .filter(
                entry -> {
                  String name = entry.toLowerCase();
                  return FORBIDDEN_JAR_FRAGMENTS.stream().anyMatch(name::contains);
                })
            .collect(Collectors.toList());

    if (!offenders.isEmpty()) {
      fail(
          "Legacy Jetty jars found on the runtime classpath; they collide with Gravitino's "
              + "Jetty 9.4.x and crash the standalone Lance REST server at startup. Exclude them "
              + "from the Hive dependencies in lance-common/build.gradle.kts. Offenders: "
              + offenders);
    }
  }

  @Test
  void testJettyContainerIsAnInterface() throws ClassNotFoundException {
    // The exact failure mode: with a conflicting Jetty, Container resolves to a class rather than
    // an interface, and Connector (9.4.x) can no longer implement it.
    Class<?> container = Class.forName("org.eclipse.jetty.util.component.Container");
    assertTrue(
        container.isInterface(),
        "org.eclipse.jetty.util.component.Container must be an interface (Jetty 9.4.x). A "
            + "conflicting legacy Jetty is on the classpath where it is a class, which breaks "
            + "org.eclipse.jetty.server.Connector at server startup.");
  }
}
