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

package org.apache.gravitino.maintenance.optimizer.updater;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class StatisticsCalculatorForTest
    implements SupportsCalculateBulkTableStatistics, SupportsCalculateBulkJobStatistics {

  public static final String NAME = "test-statistics-calculator";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public TableAndPartitionStatistics calculateTableStatistics(NameIdentifier tableIdentifier) {
    List<StatisticEntry<?>> tableStatistics = List.of(entry("row_count", 10L));
    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        Map.of(
            PartitionPath.of(List.of(new TestPartitionEntry("p1", "v1"))),
            List.of(entry("row_count", 3L)),
            PartitionPath.of(List.of(new TestPartitionEntry("p2", "v2"))),
            List.of(entry("row_count", 7L)));
    return new TableAndPartitionStatistics(tableStatistics, partitionStatistics);
  }

  @Override
  public List<StatisticEntry<?>> calculateJobStatistics(NameIdentifier jobIdentifier) {
    return List.of(entry("output_rows", 5L));
  }

  @Override
  public Map<NameIdentifier, TableAndPartitionStatistics> calculateBulkTableStatistics() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "schema", "table");
    return Map.of(identifier, calculateTableStatistics(identifier));
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> calculateAllJobStatistics() {
    NameIdentifier identifier = NameIdentifier.of("job", "sample");
    return Map.of(identifier, calculateJobStatistics(identifier));
  }

  private static StatisticEntry<?> entry(String name, long value) {
    StatisticValue statisticValue = StatisticValues.longValue(value);
    return new StatisticEntryImpl<>(name, statisticValue);
  }

  private static final class TestPartitionEntry implements PartitionEntry {
    private final String name;
    private final String value;

    private TestPartitionEntry(String name, String value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public String partitionName() {
      return name;
    }

    @Override
    public String partitionValue() {
      return value;
    }
  }
}
