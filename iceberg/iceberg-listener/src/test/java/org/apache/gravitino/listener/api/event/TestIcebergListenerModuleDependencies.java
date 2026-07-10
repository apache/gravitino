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

package org.apache.gravitino.listener.api.event;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Tests that the Iceberg listener module stays independent from Iceberg and REST APIs. */
public class TestIcebergListenerModuleDependencies {

  private static final List<String> FORBIDDEN_IMPORTS =
      List.of("org.apache.iceberg.", "org.apache.gravitino.iceberg.", "javax.servlet.");

  @Test
  public void testMainSourcesDoNotImportIcebergOrServletApis() throws IOException {
    Path sourceRoot = Path.of("src/main/java");
    if (!Files.exists(sourceRoot)) {
      return;
    }

    List<String> violations;
    try (Stream<Path> paths = Files.walk(sourceRoot)) {
      violations =
          paths
              .filter(path -> path.toString().endsWith(".java"))
              .flatMap(TestIcebergListenerModuleDependencies::forbiddenImports)
              .toList();
    }

    assertTrue(violations.isEmpty(), violations.toString());
  }

  private static Stream<String> forbiddenImports(Path path) {
    try {
      return Files.readAllLines(path).stream()
          .filter(line -> FORBIDDEN_IMPORTS.stream().anyMatch(line::contains))
          .map(line -> path + ": " + line.trim());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
