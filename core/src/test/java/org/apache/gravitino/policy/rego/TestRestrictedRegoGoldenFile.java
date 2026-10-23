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
package org.apache.gravitino.policy.rego;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Golden-file and documentation-drift tests for {@code restricted-rego-v1}. */
public class TestRestrictedRegoGoldenFile {
  private static final String GOLDEN_CASES_RESOURCE = "/restricted-rego-v1-cases.txt";
  private static final String GOLDEN_FILE =
      "core/src/test/resources/restricted-rego-v1-cases.golden";
  private static final String UPDATE_GOLDEN_ENV = "GRAVITINO_UPDATE_RESTRICTED_REGO_GOLDEN";
  private static final String REPOSITORY_ROOT_PROPERTY = "gravitino.repositoryRoot";
  private static final String GOLDEN_LICENSE_HEADER =
      "# Licensed to the Apache Software Foundation (ASF) under one or more\n"
          + "# contributor license agreements. See the NOTICE file distributed with\n"
          + "# this work for additional information regarding copyright ownership.\n"
          + "# The ASF licenses this file to You under the Apache License, Version 2.0\n"
          + "# (the \"License\"); you may not use this file except in compliance with\n"
          + "# the License. You may obtain a copy of the License at\n"
          + "#\n"
          + "#     http://www.apache.org/licenses/LICENSE-2.0\n"
          + "#\n"
          + "# Unless required by applicable law or agreed to in writing, software\n"
          + "# distributed under the License is distributed on an \"AS IS\" BASIS,\n"
          + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
          + "# See the License for the specific language governing permissions and\n"
          + "# limitations under the License.\n";

  @Test
  void testRestrictedRegoGoldenFile() throws IOException {
    String actual = renderGoldenResults();
    Path goldenFile = repositoryRoot().resolve(GOLDEN_FILE);
    if (Boolean.parseBoolean(System.getenv(UPDATE_GOLDEN_ENV))) {
      Files.writeString(goldenFile, actual, StandardCharsets.UTF_8);
    }

    String expected = Files.readString(goldenFile, StandardCharsets.UTF_8);
    Assertions.assertEquals(
        expected,
        actual,
        "Golden output changed. Review the parser behavior and regenerate with "
            + UPDATE_GOLDEN_ENV
            + "=true if the change is intentional.");
  }

  @Test
  void testParsesDocumentedPrograms() throws IOException {
    List<String> programs = documentedPrograms(repositoryRoot().resolve("docs/policies.md"));

    Assertions.assertFalse(programs.isEmpty());
    for (String program : programs) {
      Assertions.assertDoesNotThrow(() -> RestrictedRegoExpressionParserFacade.parse(program));
    }
  }

  @Test
  void testFindsRepositoryRootFromChildDirectory() {
    Path expected = repositoryRoot();

    Assertions.assertEquals(
        Optional.of(expected), findRepositoryRoot(expected.resolve("core/src/test")));
  }

  private static String renderGoldenResults() throws IOException {
    StringBuilder result = new StringBuilder(GOLDEN_LICENSE_HEADER);
    List<String> lines;
    try (InputStream input =
        TestRestrictedRegoGoldenFile.class.getResourceAsStream(GOLDEN_CASES_RESOURCE)) {
      Assertions.assertNotNull(input, "restricted Rego golden cases");
      lines = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
    }

    for (String line : lines) {
      if (line.isBlank() || line.startsWith("#")) {
        continue;
      }
      String[] fields = line.split("\\t", 2);
      Assertions.assertEquals(2, fields.length, "Invalid restricted Rego golden case: " + line);
      String name = fields[0];
      String source = fields[1];
      result.append('\n').append("=== ").append(name).append(" ===\n");
      result.append("source: ").append(escapeGoldenValue(source)).append('\n');
      try {
        RestrictedRegoProgram program = RestrictedRegoExpressionParserFacade.parse(source);
        result.append("result: ").append(escapeGoldenValue(program.toString())).append('\n');
        if (program instanceof RowFilter && ((RowFilter) program).isConditional()) {
          result
              .append("lowered: ")
              .append(escapeGoldenValue(((RowFilter) program).lower().toString()))
              .append('\n');
        }
      } catch (IllegalArgumentException exception) {
        result.append("error: ").append(escapeGoldenValue(exception.getMessage())).append('\n');
      }
    }
    return result.toString();
  }

  private static String escapeGoldenValue(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t");
  }

  private static Path repositoryRoot() {
    String repositoryRoot = System.getProperty(REPOSITORY_ROOT_PROPERTY);
    if (repositoryRoot != null && !repositoryRoot.isBlank()) {
      return Path.of(repositoryRoot).toAbsolutePath().normalize();
    }

    Path workingDirectory = Path.of("").toAbsolutePath().normalize();
    return findRepositoryRoot(workingDirectory)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Cannot find the repository root from " + workingDirectory));
  }

  private static Optional<Path> findRepositoryRoot(Path start) {
    for (Path current = start.toAbsolutePath().normalize();
        current != null;
        current = current.getParent()) {
      if (Files.isRegularFile(current.resolve("docs/policies.md"))) {
        return Optional.of(current);
      }
    }
    return Optional.empty();
  }

  private static List<String> documentedPrograms(Path policiesDocument) throws IOException {
    List<String> programs = new ArrayList<>();
    StringBuilder current = null;
    for (String line : Files.readAllLines(policiesDocument, StandardCharsets.UTF_8)) {
      if (line.equals("```restricted-rego-v1")) {
        Assertions.assertNull(current, "Nested restricted Rego documentation fence");
        current = new StringBuilder();
      } else if (current != null && line.equals("```")) {
        programs.add(current.toString().stripTrailing());
        current = null;
      } else if (current != null) {
        current.append(line).append('\n');
      }
    }
    Assertions.assertNull(current, "Unclosed restricted Rego documentation fence");
    return programs;
  }
}
