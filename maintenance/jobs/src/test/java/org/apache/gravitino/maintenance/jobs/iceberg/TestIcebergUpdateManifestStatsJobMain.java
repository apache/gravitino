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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests manifest statistics job startup in a Spark environment without Iceberg. */
public class TestIcebergUpdateManifestStatsJobMain {
  @TempDir Path tempDir;

  /** Verifies a missing Iceberg runtime fails with an actionable classpath error. */
  @Test
  public void testMissingRuntimeHasActionableError() throws Exception {
    String classpath =
        Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(entry -> !entry.contains("iceberg-spark-runtime"))
            .collect(Collectors.joining(File.pathSeparator));
    List<String> command = new ArrayList<>();
    command.add(new File(System.getProperty("java.home"), "bin/java").toString());
    command.add("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");
    command.add("-cp");
    command.add(classpath);
    command.add(IcebergUpdateManifestStatsJob.class.getName());
    command.addAll(
        Arrays.asList(
            "--catalog",
            "cli",
            "--table",
            "db.cli",
            "--updater-options",
            "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\"}",
            "--spark-conf",
            "{\"spark.master\":\"local[1]\",\"spark.ui.enabled\":\"false\","
                + "\"spark.sql.extensions\":"
                + "\"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\"}"));
    Path log = tempDir.resolve("missing-runtime.log");
    Process process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Manifest statistics job did not exit");
      String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
      assertEquals(1, process.exitValue(), output);
      assertTrue(output.contains("Missing Iceberg Spark session extensions"), output);
      assertTrue(output.contains("iceberg-spark-runtime"), output);
    } finally {
      process.destroyForcibly();
    }
  }
}
