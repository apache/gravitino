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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateJobMetrics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.stats.StatisticValues;

public class DummyJobMetricsCalculator implements SupportsCalculateJobMetrics {

  public static final String DUMMY_JOB_METRICS = "dummy-job-metrics";
  public static final String JOB_STAT_NAME = "dummy-job-stat-name";

  @Override
  public String name() {
    return DUMMY_JOB_METRICS;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public List<MetricPoint> calculateJobMetrics(NameIdentifier jobIdentifier) {
    long timestampSeconds = System.currentTimeMillis() / 1000;
    return List.of(
        MetricPoint.forJob(
            jobIdentifier, JOB_STAT_NAME, StatisticValues.longValue(1L), timestampSeconds));
  }
}
