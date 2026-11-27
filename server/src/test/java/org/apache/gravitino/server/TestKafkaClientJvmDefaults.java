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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the Kafka client JVM defaults that {@code bin/gravitino.sh.template} adds through {@code
 * add_default_jvm_system_property} in {@code bin/common.sh.template}. Each case starts a JVM the
 * way the launcher does, with {@code JAVA_OPTS} on the command line, and checks the effective
 * system properties. The command line overrides {@code JAVA_TOOL_OPTIONS} and {@code
 * JDK_JAVA_OPTIONS}, so the check has to run against a real JVM.
 */
class TestKafkaClientJvmDefaults {

  private static final String CONFIG_PROVIDERS = "org.apache.kafka.automatic.config.providers";
  private static final String ALLOWED_URLS = "org.apache.kafka.sasl.oauthbearer.allowed.urls";
  private static final String TOKEN_URL = "https://idp.example.com/token";
  private static final List<String> JVM_OPTION_VARIABLES =
      List.of("JAVA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS");
  private static final String MARKER = "gravitino.test.marker";
  private static final Pattern PROPERTY_LINE =
      Pattern.compile("^\\s+((?:org\\.apache\\.kafka|gravitino\\.test)\\.\\S+) = (.*)$");
  private static final long TIMEOUT_SECONDS = 60;

  @TempDir Path dir;

  @Test
  void testLauncherSetsBothDefaults() throws Exception {
    List<String> calls = launcherCalls();
    assertTrue(calls.stream().anyMatch(call -> call.contains(CONFIG_PROVIDERS)), calls::toString);
    assertTrue(calls.stream().anyMatch(call -> call.contains(ALLOWED_URLS)), calls::toString);

    Map<String, String> properties = effectiveProperties(ImmutableMap.of());
    assertEquals("none", properties.get(CONFIG_PROVIDERS));
    assertEquals("", properties.get(ALLOWED_URLS));
  }

  @Test
  void testKeepsAllowListFromJavaOpts() throws Exception {
    assertAllowList("JAVA_OPTS", "-D" + ALLOWED_URLS + "=" + TOKEN_URL);
  }

  @Test
  void testKeepsAllowListFromJavaToolOptions() throws Exception {
    assertAllowList("JAVA_TOOL_OPTIONS", "-D" + ALLOWED_URLS + "=" + TOKEN_URL);
  }

  @Test
  void testKeepsDoubleQuotedAllowListFromJavaToolOptions() throws Exception {
    assertAllowList(
        "JAVA_TOOL_OPTIONS",
        "-Djavax.net.ssl.trustStore=/tmp/ts \"-D" + ALLOWED_URLS + "=" + TOKEN_URL + "\"");
  }

  @Test
  void testKeepsSingleQuotedAllowListFromJavaToolOptions() throws Exception {
    assertAllowList("JAVA_TOOL_OPTIONS", "'-D" + ALLOWED_URLS + "=" + TOKEN_URL + "'");
  }

  @Test
  void testKeepsAllowListFromJdkJavaOptions() throws Exception {
    assertAllowList("JDK_JAVA_OPTIONS", "-D" + ALLOWED_URLS + "=" + TOKEN_URL);
  }

  @Test
  void testKeepsAllowListFromJdkJavaOptionsArgFile() throws Exception {
    Path argFile = dir.resolve("kafka.args");
    Files.write(
        argFile, ("-D" + ALLOWED_URLS + "=" + TOKEN_URL + "\n").getBytes(StandardCharsets.UTF_8));
    assertAllowList("JDK_JAVA_OPTIONS", "@" + argFile);
  }

  @Test
  void testKeepsAllowListFromUnderscoreJavaOptions() throws Exception {
    assertAllowList("_JAVA_OPTIONS", "-D" + ALLOWED_URLS + "=" + TOKEN_URL);
  }

  @Test
  void testKeepsConfigProvidersFromJavaToolOptions() throws Exception {
    Map<String, String> properties =
        effectiveProperties(
            ImmutableMap.of("JAVA_TOOL_OPTIONS", "'-D" + CONFIG_PROVIDERS + "=file'"));
    assertEquals("file", properties.get(CONFIG_PROVIDERS));
    assertEquals("", properties.get(ALLOWED_URLS));
  }

  @Test
  void testKeepsExistingJavaToolOptions() throws Exception {
    Map<String, String> properties =
        effectiveProperties(ImmutableMap.of("JAVA_TOOL_OPTIONS", "-D" + MARKER + "=kept"));
    assertEquals("kept", properties.get(MARKER));
    assertEquals("none", properties.get(CONFIG_PROVIDERS));
    assertEquals("", properties.get(ALLOWED_URLS));
  }

  @Test
  void testIgnoresSimilarPropertyNames() throws Exception {
    Map<String, String> properties =
        effectiveProperties(
            ImmutableMap.of("JAVA_TOOL_OPTIONS", "-D" + ALLOWED_URLS + "X=https://a.example.com"));
    assertEquals("", properties.get(ALLOWED_URLS));
    assertEquals("none", properties.get(CONFIG_PROVIDERS));
  }

  private void assertAllowList(String variable, String options) throws Exception {
    Map<String, String> properties = effectiveProperties(ImmutableMap.of(variable, options));
    assertEquals(TOKEN_URL, properties.get(ALLOWED_URLS));
    assertEquals("none", properties.get(CONFIG_PROVIDERS));
  }

  /** Returns the {@code add_default_jvm_system_property} lines of the server launcher. */
  private static List<String> launcherCalls() throws IOException {
    Path launcher = TestLog4j2RollingConfiguration.rootDir().resolve("bin/gravitino.sh.template");
    List<String> calls =
        Files.readAllLines(launcher, StandardCharsets.UTF_8).stream()
            .map(String::trim)
            .filter(line -> line.startsWith("add_default_jvm_system_property "))
            .collect(Collectors.toList());
    assertFalse(calls.isEmpty(), "no add_default_jvm_system_property call in " + launcher);
    return calls;
  }

  /**
   * Runs the launcher's default calls with the given JVM option variables, starts a JVM with the
   * resulting environment and {@code JAVA_OPTS} on the command line, and returns its system
   * properties.
   */
  private Map<String, String> effectiveProperties(Map<String, String> jvmOptions) throws Exception {
    String script =
        ". \"${COMMON_SH}\" > /dev/null && "
            + String.join(" && ", launcherCalls())
            + " && \"${JAVA_BIN}\" ${JAVA_OPTS} -XshowSettings:properties -version";
    Path outputFile = Files.createTempFile(dir, "launcher-check", ".out");
    ProcessBuilder builder = new ProcessBuilder("bash", "-c", script);
    builder.redirectErrorStream(true);
    builder.redirectOutput(outputFile.toFile());
    Map<String, String> env = builder.environment();
    JVM_OPTION_VARIABLES.forEach(env::remove);
    env.putAll(jvmOptions);
    env.put(
        "COMMON_SH",
        TestLog4j2RollingConfiguration.rootDir().resolve("bin/common.sh.template").toString());
    env.put("JAVA_BIN", Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    env.put("GRAVITINO_HOME", dir.toString());
    env.put("GRAVITINO_CONF_DIR", dir.resolve("conf").toString());
    env.put("GRAVITINO_LOG_DIR", dir.toString());
    env.put("GRAVITINO_VERSION", "test");

    Process process = builder.start();
    if (!process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      process.descendants().forEach(ProcessHandle::destroyForcibly);
      process.destroyForcibly();
      process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      throw new AssertionError(
          "launcher check timed out after "
              + TIMEOUT_SECONDS
              + "s: "
              + new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8));
    }
    String output = new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8);
    assertEquals(0, process.exitValue(), "launcher check failed: " + output);

    Map<String, String> properties = new HashMap<>();
    for (String line : output.split("\\R")) {
      Matcher matcher = PROPERTY_LINE.matcher(line);
      if (matcher.matches()) {
        properties.put(matcher.group(1), matcher.group(2));
      }
    }
    assertTrue(properties.containsKey(ALLOWED_URLS), "property missing in output: " + output);
    return properties;
  }
}
