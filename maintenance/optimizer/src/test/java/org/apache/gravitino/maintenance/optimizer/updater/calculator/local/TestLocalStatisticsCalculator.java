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

package org.apache.gravitino.maintenance.optimizer.updater.calculator.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestLocalStatisticsCalculator {

  @TempDir Path tempDir;

  @Test
  void testComputeTableStatisticsUsesReader() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":2}",
            "{\"identifier\":\"other\",\"stats-type\":\"table\",\"s1\":100}"));

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(statsFile.toString(), null);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals("s1", stats.get(0).name());
    Assertions.assertEquals(2L, stats.get(0).value().value());
  }

  @Test
  void testComputeAllTableStatistics() throws IOException {
    Path statsFile = tempDir.resolve("stats.json");
    Files.write(
        statsFile,
        List.of(
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,\"s2\":5}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":3}",
            "{\"identifier\":\"catalog.schema.other\",\"stats-type\":\"table\",\"s1\":10}"));

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(statsFile.toString(), null);
    calculator.initialize(env);

    Map<NameIdentifier, TableAndPartitionStatistics> allStatistics =
        calculator.calculateBulkTableStatistics();
    Assertions.assertEquals(2, allStatistics.size());

    Map<String, StatisticEntry<?>> tableStats =
        toNameMap(
            allStatistics.get(NameIdentifier.parse("catalog.schema.table")).tableStatistics());
    Assertions.assertEquals(2, tableStats.size());
    Assertions.assertEquals(3L, tableStats.get("s1").value().value());
    Assertions.assertEquals(5L, tableStats.get("s2").value().value());

    Map<String, StatisticEntry<?>> otherStats =
        toNameMap(
            allStatistics.get(NameIdentifier.parse("catalog.schema.other")).tableStatistics());
    Assertions.assertEquals(1, otherStats.size());
    Assertions.assertEquals(10L, otherStats.get("s1").value().value());
  }

  @Test
  void testComputeTableStatisticsFromPayload() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,\"s2\":5}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":3}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(3L, map.get("s1").value().value());
    Assertions.assertEquals(5L, map.get("s2").value().value());
  }

  @Test
  void testSkipNonNumericTextualStatistics() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1,"
                + "\"text_metric\":\"abc\"}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s2\":\"2.5\"}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(1L, map.get("s1").value().value());
    Assertions.assertEquals(2.5D, map.get("s2").value().value());
    Assertions.assertFalse(map.containsKey("text_metric"));
  }

  @Test
  void testComputeJobStatisticsFromPayload() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.job1\",\"stats-type\":\"job\",\"duration\":10}",
            "{\"identifier\":\"catalog.schema.job1\",\"stats-type\":\"job\",\"duration\":20,"
                + "\"planning\":5}",
            "{\"identifier\":\"catalog.schema.job2\",\"stats-type\":\"job\",\"duration\":1}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator.calculateJobStatistics(NameIdentifier.parse("catalog.schema.job1"));
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(20L, map.get("duration").value().value());
    Assertions.assertEquals(5L, map.get("planning").value().value());
  }

  @Test
  void testJobIdentifierWithMultipleDotsIsSupported() {
    String payload =
        "{\"identifier\":\"org.team.pipeline.stage.job42\",\"stats-type\":\"job\",\"duration\":10}";

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator.calculateJobStatistics(NameIdentifier.parse("org.team.pipeline.stage.job42"));
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals(10L, stats.get(0).value().value());
  }

  @Test
  void testTableIdentifierWithMultipleDotsIsStillIgnored() {
    String payload =
        "{\"identifier\":\"catalog.db.schema.extra.table\",\"stats-type\":\"table\",\"rows\":10}";

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.db.schema.extra.table"))
            .tableStatistics();
    Assertions.assertTrue(stats.isEmpty());
  }

  @Test
  void testComputePartitionStatisticsFromPayload() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":\"2024-01-01\"},\"rows\":10,\"size\":100}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":\"2024-01-01\"},\"rows\":20,\"size\":200}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":\"2024-01-02\"},\"rows\":5}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .partitionStatistics();

    Assertions.assertEquals(2, partitionStatistics.size());

    PartitionPath p1 = PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2024-01-01")));
    Map<String, StatisticEntry<?>> p1Stats = toNameMap(partitionStatistics.get(p1));
    Assertions.assertEquals(2, p1Stats.size());
    Assertions.assertEquals(20L, p1Stats.get("rows").value().value());
    Assertions.assertEquals(200L, p1Stats.get("size").value().value());

    PartitionPath p2 = PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2024-01-02")));
    Map<String, StatisticEntry<?>> p2Stats = toNameMap(partitionStatistics.get(p2));
    Assertions.assertEquals(1, p2Stats.size());
    Assertions.assertEquals(5L, p2Stats.get("rows").value().value());
  }

  @Test
  void testComputeAllPartitionStatistics() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table1\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":\"2024-01-01\"},\"rows\":10}",
            "{\"identifier\":\"catalog.schema.table2\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":\"2024-01-02\"},\"rows\":30}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    Map<NameIdentifier, TableAndPartitionStatistics> allStatistics =
        calculator.calculateBulkTableStatistics();
    Assertions.assertEquals(2, allStatistics.size());

    PartitionPath p1 = PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2024-01-01")));
    Map<PartitionPath, List<StatisticEntry<?>>> table1Partitions =
        allStatistics.get(NameIdentifier.parse("catalog.schema.table1")).partitionStatistics();
    Assertions.assertEquals(1, table1Partitions.size());
    Assertions.assertEquals(10L, toNameMap(table1Partitions.get(p1)).get("rows").value().value());
    Assertions.assertTrue(
        allStatistics
            .get(NameIdentifier.parse("catalog.schema.table1"))
            .tableStatistics()
            .isEmpty());

    PartitionPath p2 = PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2024-01-02")));
    Map<PartitionPath, List<StatisticEntry<?>>> table2Partitions =
        allStatistics.get(NameIdentifier.parse("catalog.schema.table2")).partitionStatistics();
    Assertions.assertEquals(1, table2Partitions.size());
    Assertions.assertEquals(30L, toNameMap(table2Partitions.get(p2)).get("rows").value().value());
    Assertions.assertTrue(
        allStatistics
            .get(NameIdentifier.parse("catalog.schema.table2"))
            .tableStatistics()
            .isEmpty());
  }

  @Test
  void testComputeAllJobStatistics() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.job1\",\"stats-type\":\"job\",\"duration\":10}",
            "{\"identifier\":\"catalog.schema.job2\",\"stats-type\":\"job\",\"duration\":7}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    Map<NameIdentifier, List<StatisticEntry<?>>> allStatistics =
        calculator.calculateAllJobStatistics();
    Assertions.assertEquals(2, allStatistics.size());
    Assertions.assertEquals(
        10L,
        toNameMap(allStatistics.get(NameIdentifier.parse("catalog.schema.job1")))
            .get("duration")
            .value()
            .value());
    Assertions.assertEquals(
        7L,
        toNameMap(allStatistics.get(NameIdentifier.parse("catalog.schema.job2")))
            .get("duration")
            .value()
            .value());
  }

  @Test
  void testSkipMalformedJsonLine() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s2\":2}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(1L, map.get("s1").value().value());
    Assertions.assertEquals(2L, map.get("s2").value().value());
  }

  @Test
  void testSkipRecordWithMissingOrInvalidStatsType() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"s1\":1}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"unknown\",\"s2\":2}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s3\":3}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .tableStatistics();
    Map<String, StatisticEntry<?>> map = toNameMap(stats);
    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals(3L, map.get("s3").value().value());
  }

  @Test
  void testSkipPartitionRecordWithInvalidPartitionPath() {
    String payload =
        String.join(
            "\n",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
                + "\"partition-path\":\"dt=2024-01-01\",\"rows\":1}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":100},\"rows\":2}",
            "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
                + "\"partition-path\":{\"dt\":\"2024-01-02\"},\"rows\":3}");

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .partitionStatistics();
    Assertions.assertEquals(1, partitionStatistics.size());

    PartitionPath valid = PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2024-01-02")));
    Assertions.assertEquals(
        3L, toNameMap(partitionStatistics.get(valid)).get("rows").value().value());
  }

  @Test
  void testPartitionPathOrderIsPreserved() {
    String payload =
        "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"partition\","
            + "\"partition-path\":{\"p1\":\"v1\",\"p2\":\"v2\"},\"rows\":1}";

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        calculator
            .calculateTableStatistics(NameIdentifier.parse("catalog.schema.table"))
            .partitionStatistics();
    Assertions.assertEquals(1, partitionStatistics.size());

    PartitionPath actual = partitionStatistics.keySet().iterator().next();
    Assertions.assertEquals(2, actual.entries().size());
    Assertions.assertEquals("p1", actual.entries().get(0).partitionName());
    Assertions.assertEquals("v1", actual.entries().get(0).partitionValue());
    Assertions.assertEquals("p2", actual.entries().get(1).partitionName());
    Assertions.assertEquals("v2", actual.entries().get(1).partitionValue());
  }

  @Test
  void testJobIdentifiersDoNotApplyDefaultCatalog() {
    String payload = "{\"identifier\":\"job-1\",\"stats-type\":\"job\",\"duration\":10}";

    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    List<StatisticEntry<?>> stats =
        calculator.calculateJobStatistics(NameIdentifier.parse("job-1"));
    Assertions.assertEquals(1, stats.size());
    Assertions.assertEquals(10L, stats.get(0).value().value());
    Assertions.assertTrue(
        calculator.calculateJobStatistics(NameIdentifier.parse("catalog.job-1")).isEmpty());
  }

  @Test
  void testCalculateWithoutInitializeFails() {
    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> calculator.calculateTableStatistics(NameIdentifier.parse("catalog.db.table")));
    Assertions.assertThrows(IllegalStateException.class, calculator::calculateBulkTableStatistics);
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> calculator.calculateJobStatistics(NameIdentifier.parse("catalog.db.job")));
    Assertions.assertThrows(IllegalStateException.class, calculator::calculateAllJobStatistics);
  }

  @Test
  void testNullIdentifierFailsFast() {
    String payload = "{\"identifier\":\"catalog.schema.table\",\"stats-type\":\"table\",\"s1\":1}";
    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, payload);
    calculator.initialize(env);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> calculator.calculateTableStatistics(null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> calculator.calculateJobStatistics(null));
  }

  @Test
  void testInitializeWithoutFilePathAndPayloadFails() {
    LocalStatisticsCalculator calculator = new LocalStatisticsCalculator();
    OptimizerEnv env = createEnv(null, null);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> calculator.initialize(env));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("LocalStatisticsCalculator requires runtime statistics input content."));
  }

  private OptimizerEnv createEnv(String statisticsFilePath, String statisticsPayload) {
    Map<String, String> configs = new HashMap<>();
    configs.put(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG.getKey(), "catalog");
    OptimizerEnv env = new OptimizerEnv(new OptimizerConfig(configs));
    if (statisticsFilePath != null) {
      return env.withContent(StatisticsInputContent.fromFilePath(statisticsFilePath));
    }
    if (statisticsPayload != null) {
      return env.withContent(StatisticsInputContent.fromPayload(statisticsPayload));
    }
    return env;
  }

  private Map<String, StatisticEntry<?>> toNameMap(List<StatisticEntry<?>> stats) {
    Map<String, StatisticEntry<?>> map = new HashMap<>();
    for (StatisticEntry<?> stat : stats) {
      map.put(stat.name(), stat);
    }
    return map;
  }
}
