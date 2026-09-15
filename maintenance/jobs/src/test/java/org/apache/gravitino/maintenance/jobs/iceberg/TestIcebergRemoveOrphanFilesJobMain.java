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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIcebergRemoveOrphanFilesJobMain {
  @TempDir Path tempDir;

  @Test
  void testNamedArgumentsRunPreviewAndCleanup() throws Exception {
    Path table = tempDir.resolve("db/cli");
    new HadoopTables(new Configuration())
        .create(
            new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
            table.toString());
    Path data = Files.createDirectories(table.resolve("data"));
    Path orphan = Files.write(data.resolve("orphan"), new byte[] {1});
    Files.setLastModifiedTime(orphan, FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS)));
    Map<String, String> config = new HashMap<>();
    config.put("spark.master", "local[2]");
    config.put("spark.ui.enabled", "false");
    config.put("spark.sql.shuffle.partitions", "2");
    config.put(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    config.put("spark.sql.catalog.cli", "org.apache.iceberg.spark.SparkCatalog");
    config.put("spark.sql.catalog.cli.type", "hadoop");
    config.put("spark.sql.catalog.cli.warehouse", tempDir.toString());
    String json = new ObjectMapper().writeValueAsString(config);
    IcebergRemoveOrphanFilesJob.main(
        new String[] {"--catalog", "cli", "--table", "db.cli", "--spark-conf", json, "--dry-run"});
    assertTrue(Files.exists(orphan));
    IcebergRemoveOrphanFilesJob.main(
        new String[] {
          "--catalog", "cli", "--table", "db.cli", "--spark-conf", json, "--dry-run", "false"
        });
    assertFalse(Files.exists(orphan));
  }

  @Test
  void testCliFailureExitAndUsage() throws Exception {
    String output = runFailure(false, new String[] {"--catalog", "cli"});
    assertTrue(output.contains("Usage: IcebergRemoveOrphanFilesJob"), output);
    assertTrue(output.contains("--table is required"), output);
  }

  @Test
  void testMissingRuntimeHasActionableError() throws Exception {
    String output =
        runFailure(
            true,
            new String[] {
              "--catalog",
              "cli",
              "--table",
              "db.cli",
              "--spark-conf",
              "{\"spark.master\":\"local[1]\",\"spark.ui.enabled\":\"false\"}"
            });
    assertTrue(output.contains("iceberg-spark-runtime"), output);
  }

  private String runFailure(boolean omitIcebergRuntime, String[] args) throws Exception {
    String classpath =
        Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(entry -> !omitIcebergRuntime || !entry.contains("iceberg-spark-runtime"))
            .collect(Collectors.joining(File.pathSeparator));
    List<String> command = new ArrayList<>();
    command.add(new File(System.getProperty("java.home"), "bin/java").toString());
    command.add("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");
    command.add("-cp");
    command.add(classpath);
    command.add(IcebergRemoveOrphanFilesJob.class.getName());
    command.addAll(Arrays.asList(args));
    Path log = Files.createTempFile(tempDir, "cli-", ".log");
    Process process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(45, TimeUnit.SECONDS), "CLI did not exit");
      String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
      assertEquals(1, process.exitValue(), output);
      return output;
    } finally {
      process.destroyForcibly();
    }
  }
}
