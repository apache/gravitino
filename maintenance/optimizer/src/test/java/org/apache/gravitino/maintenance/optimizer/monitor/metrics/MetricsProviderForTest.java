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

package org.apache.gravitino.maintenance.optimizer.monitor.metrics;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.monitor.job.TableJobRelationProviderForTest;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class MetricsProviderForTest implements MetricsProvider {

  public static final String NAME = "metrics-provider-for-test";
  public static final AtomicInteger PARTITION_METRICS_CALLS = new AtomicInteger();
  public static volatile PartitionPath LAST_PARTITION_PATH = null;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public Map<String, List<MetricSample>> jobMetrics(
      NameIdentifier jobIdentifier, long startTime, long endTime) {
    if (TableJobRelationProviderForTest.JOB1.equals(jobIdentifier)) {
      return Map.of(
          "duration",
          List.of(
              metric(99, "duration", StatisticValues.longValue(10L)),
              metric(102, "duration", StatisticValues.longValue(20L))));
    }
    if (TableJobRelationProviderForTest.JOB2.equals(jobIdentifier)) {
      return Map.of(
          "duration",
          List.of(
              metric(98, "duration", StatisticValues.longValue(30L)),
              metric(104, "duration", StatisticValues.longValue(40L))));
    }
    return Map.of();
  }

  @Override
  public Map<String, List<MetricSample>> tableMetrics(
      NameIdentifier tableIdentifier, long startTime, long endTime) {
    return Map.of(
        "row_count",
        List.of(
            metric(95, "row_count", StatisticValues.longValue(100L)),
            metric(100, "row_count", StatisticValues.longValue(200L))));
  }

  @Override
  public Map<String, List<MetricSample>> partitionMetrics(
      NameIdentifier tableIdentifier, PartitionPath partitionPath, long startTime, long endTime) {
    PARTITION_METRICS_CALLS.incrementAndGet();
    LAST_PARTITION_PATH = partitionPath;
    return Map.of(
        "row_count",
        List.of(
            metric(97, "row_count", StatisticValues.longValue(110L)),
            metric(101, "row_count", StatisticValues.longValue(210L))));
  }

  @Override
  public void close() throws Exception {}

  public static void reset() {
    PARTITION_METRICS_CALLS.set(0);
    LAST_PARTITION_PATH = null;
  }

  private static <T> MetricSample metric(
      long timestamp, String metricName, StatisticValue<T> statisticValue) {
    return new MetricSample() {
      @Override
      public long timestamp() {
        return timestamp;
      }

      @Override
      public StatisticEntry<?> statistic() {
        return new StatisticEntry<T>() {
          @Override
          public String name() {
            return metricName;
          }

          @Override
          public StatisticValue<T> value() {
            return statisticValue;
          }
        };
      }
    };
  }
}
