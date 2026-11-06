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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises the distribution launcher, configuration loading, and existing-file protection. */
public class TestPolicyBindingMigrationTool {

  @TempDir private Path directory;

  /** Runs the actual launcher with an upgraded H2 database in each supported mode. */
  @Test
  public void testDistributionLauncher() throws Exception {
    Path repository = Paths.get(System.getProperty("gravitino.repositoryRoot"));
    Path bin = Files.createDirectories(directory.resolve("bin"));
    Files.copy(repository.resolve("bin/common.sh.template"), bin.resolve("common.sh"));
    Files.copy(
        repository.resolve("bin/migrate-policy-bindings.sh.template"),
        bin.resolve("migrate-policy-bindings.sh"));
    Path conf = Files.createDirectories(directory.resolve("conf"));
    Files.writeString(
        conf.resolve("gravitino-env.sh"),
        "export GRAVITINO_VERSION=test\n",
        StandardCharsets.UTF_8);
    String url = "jdbc:h2:file:" + directory.resolve("db") + ";MODE=MYSQL;AUTO_SERVER=FALSE";
    Files.writeString(
        conf.resolve("gravitino.conf"),
        "gravitino.entity.store.relational.jdbcUrl="
            + url
            + "\n"
            + "gravitino.entity.store.relational.jdbcDriver=org.h2.Driver\n"
            + "gravitino.entity.store.relational.jdbcUser=test\n"
            + "gravitino.entity.store.relational.jdbcPassword=test\n",
        StandardCharsets.UTF_8);
    assertLaunchSucceeds(bin, "--help");
    assertFalse(Files.exists(directory.resolve("db.mv.db")));
    assertTrue(launch(bin, "--dry-run") != 0);
    assertFalse(Files.exists(directory.resolve("db.mv.db")));
    try (Connection connection = DriverManager.getConnection(url, "test", "test");
        Reader schema =
            Files.newBufferedReader(
                repository.resolve("scripts/h2/schema-2.0.0-h2.sql"), StandardCharsets.UTF_8)) {
      RunScript.execute(connection, schema);
    }
    assertLaunchSucceeds(bin, "--dry-run");
    assertLaunchSucceeds(bin);
    assertLaunchSucceeds(bin, "--verify");
    assertTrue(launch(bin, "--unknown") != 0);
  }

  /** The tool only accepts migration modes, never arbitrary object or policy parameters. */
  @Test
  public void testMigrationModes() {
    assertEquals("", PolicyBindingMigrationTool.mode(new String[0]));
    for (String mode : new String[] {"--dry-run", "--verify", "--help"}) {
      assertEquals(mode, PolicyBindingMigrationTool.mode(new String[] {mode}));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> PolicyBindingMigrationTool.mode(new String[] {"--dry-run", "--verify"}));
    assertThrows(
        IllegalArgumentException.class,
        () -> PolicyBindingMigrationTool.mode(new String[] {"policy-name"}));
  }

  private void assertLaunchSucceeds(Path bin, String... args) throws Exception {
    int exitCode = launch(bin, args);
    assertEquals(0, exitCode, Files.readString(directory.resolve("launcher.log")));
  }

  private int launch(Path bin, String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add("bash");
    command.add(bin.resolve("migrate-policy-bindings.sh").toString());
    command.addAll(Arrays.asList(args));
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().put("GRAVITINO_HOME", directory.toString());
    builder.environment().put("GRAVITINO_CONF_DIR", directory.resolve("conf").toString());
    builder.environment().put("GRAVITINO_CLASSPATH", System.getProperty("java.class.path"));
    builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
    Path output = directory.resolve("launcher.log");
    builder.redirectErrorStream(true).redirectOutput(output.toFile());
    Process process = builder.start();
    if (!process.waitFor(45, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new AssertionError("Migration launcher timed out: " + Files.readString(output));
    }
    return process.exitValue();
  }
}
