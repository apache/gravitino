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
package org.apache.gravitino.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Verifies module access options at each packaged launch script's JVM invocation. */
class TestLaunchScriptJavaOptions {

  private static final List<String> SCRIPTS =
      List.of(
          "gravitino",
          "gravitino-iceberg-rest-server",
          "gravitino-lance-rest-server",
          "gravitino-optimizer");

  @TempDir Path dir;

  @ParameterizedTest(name = "{0} with Java {1}")
  @MethodSource("supportedRuntimes")
  void testAcceptedJavaVersionsReceiveModuleOptions(String script, String version)
      throws Exception {
    assertEquals(0, launch(script, version));
    List<String> arguments = Files.readAllLines(dir.resolve("java-arguments"));
    int nioOption = arguments.indexOf("java.base/java.nio=ALL-UNNAMED");
    assertTrue(nioOption > 0, "Arrow requires java.nio to be opened: " + arguments);
    assertEquals("--add-opens", arguments.get(nioOption - 1));
    assertTrue(arguments.contains("-XX:+IgnoreUnrecognizedVMOptions"));
    assertTrue(arguments.contains("java.base/java.lang=ALL-UNNAMED"));
    assertTrue(arguments.contains("-Dlaunch.script.test=preserved"));
  }

  @ParameterizedTest(name = "{0} rejects Java {1}")
  @MethodSource("unsupportedRuntimes")
  void testOlderJavaVersionsAreRejectedBeforeLaunch(String script, String version)
      throws Exception {
    assertEquals(1, launch(script, version));
    assertFalse(Files.exists(dir.resolve("java-arguments")));
    assertTrue(Files.readString(dir.resolve("output")).contains("requires Java 17 or newer"));
  }

  private int launch(String script, String version) throws Exception {
    Path bin = Files.createDirectories(dir.resolve("bin"));
    Path source = TestLog4j2RollingConfiguration.rootDir().resolve("bin");
    Files.copy(source.resolve("common.sh.template"), bin.resolve("common.sh"));
    Path launcher = bin.resolve(script + ".sh");
    Files.copy(source.resolve(script + ".sh.template"), launcher);
    Path javaBin = Files.createDirectories(dir.resolve("jdk/bin"));
    Path java = javaBin.resolve("java");
    Files.writeString(
        java,
        "#!/bin/bash\n"
            + "if [[ \"$1\" == \"-version\" ]]; then\n"
            + "  echo \"openjdk version \\\"${TEST_JAVA_VERSION}\\\"\" >&2\n"
            + "  exit 0\n"
            + "fi\n"
            + "printf '%s\\n' \"$@\" > \"${JAVA_ARGUMENTS_FILE}\"\n",
        StandardCharsets.UTF_8);
    assertTrue(java.toFile().setExecutable(true));
    ProcessBuilder builder = new ProcessBuilder("bash", launcher.toString(), "run");
    Map<String, String> env = builder.environment();
    env.put("JAVA_HOME", javaBin.getParent().toString());
    env.put("TEST_JAVA_VERSION", version);
    env.put("JAVA_ARGUMENTS_FILE", dir.resolve("java-arguments").toString());
    env.put("JAVA_OPTS", "-Dlaunch.script.test=preserved");
    env.put("GRAVITINO_HOME", dir.toString());
    env.put("GRAVITINO_CONF_DIR", dir.resolve("conf").toString());
    env.put("GRAVITINO_LOG_DIR", dir.resolve("logs").toString());
    env.put("GRAVITINO_VERSION", "test");
    Path output = dir.resolve("output");
    Process process = builder.redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Launch script timed out: " + script);
      return process.exitValue();
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  private static Stream<Arguments> supportedRuntimes() {
    return runtimes("17.0.8", "21.0.4", "25-ea");
  }

  private static Stream<Arguments> unsupportedRuntimes() {
    return runtimes("1.8.0_402", "11.0.22", "16.0.2");
  }

  private static Stream<Arguments> runtimes(String... versions) {
    return SCRIPTS.stream()
        .flatMap(script -> Stream.of(versions).map(version -> Arguments.of(script, version)));
  }
}
