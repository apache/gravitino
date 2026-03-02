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

package org.apache.gravitino.maintenance.optimizer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.MetricsProviderForTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOptimizerCmd {

  @Test
  void testGlobalHelpNoLongerIncludesPlannedCommands() {
    String[] output = runCommand("--help");
    Assertions.assertFalse(output[0].contains("Planned commands (not implemented):"));
  }

  @Test
  void testCommandHelpPrintsCommandScopedInfoOnly() {
    String[] output = runCommand("--help", "--type", "update-statistics");
    Assertions.assertTrue(output[0].contains("Command: update-statistics"));
    Assertions.assertTrue(output[0].contains("Required options: --calculator-name"));
    Assertions.assertFalse(output[0].contains("Planned commands (not implemented):"));
  }

  @Test
  void testRejectStatisticsInputWhenCalculatorIsNotLocal() {
    String[] output =
        runCommand(
            "--type",
            "update-statistics",
            "--calculator-name",
            "mock-calculator",
            "--statistics-payload",
            "{\"identifier\":\"c.db.t\"}");
    Assertions.assertTrue(
        output[1].contains(
            "--statistics-payload and --file-path are only supported when --calculator-name is local-stats-calculator."));
  }

  @Test
  void testRequireStatisticsInputForLocalCalculator() {
    String[] output =
        runCommand("--type", "update-statistics", "--calculator-name", "local-stats-calculator");
    Assertions.assertTrue(
        output[1].contains(
            "Command 'update-statistics' with --calculator-name local-stats-calculator requires one of --statistics-payload or --file-path."));
  }

  @Test
  void testTypeMustUseKebabCase() {
    String[] output = runCommand("--type", "update_statistics");
    Assertions.assertTrue(
        output[1].contains(
            "Invalid --type: update_statistics. Use kebab-case format, for example: update-statistics."));
  }

  @Test
  void testRejectUnsupportedOptionForMonitorMetrics() {
    String[] output =
        runCommand(
            "--type",
            "monitor-metrics",
            "--identifiers",
            "catalog.db.table",
            "--action-time",
            "1",
            "--calculator-name",
            "local-stats-calculator");
    Assertions.assertTrue(
        output[1].contains("Unsupported options for command 'monitor-metrics': --calculator-name"));
  }

  @Test
  void testListTableMetricsImplemented() throws Exception {
    Path confPath = createOptimizerConfForMetricsProvider();
    String[] output =
        runCommand(
            "--type",
            "list-table-metrics",
            "--identifiers",
            "test.db.table",
            "--conf-path",
            confPath.toString());
    Assertions.assertTrue(output[1].isEmpty(), "stderr=" + output[1] + ", stdout=" + output[0]);
    Assertions.assertTrue(output[0].contains("MetricsResult{scopeType=TABLE"));
    Assertions.assertTrue(output[0].contains("identifier=test.db.table"));
    Assertions.assertTrue(output[0].contains("row_count=["));
  }

  @Test
  void testListJobMetricsImplemented() throws Exception {
    Path confPath = createOptimizerConfForMetricsProvider();
    String[] output =
        runCommand(
            "--type",
            "list-job-metrics",
            "--identifiers",
            "test.db.job1",
            "--conf-path",
            confPath.toString());
    Assertions.assertTrue(output[1].isEmpty(), "stderr=" + output[1] + ", stdout=" + output[0]);
    Assertions.assertTrue(output[0].contains("MetricsResult{scopeType=JOB"));
    Assertions.assertTrue(output[0].contains("identifier=test.db.job1"));
    Assertions.assertTrue(output[0].contains("duration=["));
  }

  private Path createOptimizerConfForMetricsProvider() throws Exception {
    Path confPath = Files.createTempFile("optimizer-test-", ".conf");
    String content =
        String.join(
                System.lineSeparator(),
                "gravitino.optimizer.gravitinoUri = http://localhost:8090",
                "gravitino.optimizer.gravitinoMetalake = test",
                "gravitino.optimizer.monitor.metricsProvider = " + MetricsProviderForTest.NAME)
            + System.lineSeparator();
    Files.writeString(confPath, content, StandardCharsets.UTF_8);
    confPath.toFile().deleteOnExit();
    return confPath;
  }

  private String[] runCommand(String... args) {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outBuffer, true, StandardCharsets.UTF_8);
    PrintStream err = new PrintStream(errBuffer, true, StandardCharsets.UTF_8);
    OptimizerCmd.run(args, out, err);
    return new String[] {
      outBuffer.toString(StandardCharsets.UTF_8), errBuffer.toString(StandardCharsets.UTF_8)
    };
  }
}
