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

package org.apache.gravitino.maintenance.optimizer.monitor.evaluator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.monitor.job.JobProviderForTest;

public class MetricsEvaluatorForTest implements MetricsEvaluator {

  public static final String NAME = "test-metrics-evaluator";
  public static final AtomicInteger INVOCATIONS = new AtomicInteger();
  private static volatile boolean FAIL_JOB2 = false;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean evaluateMetrics(
      MetricScope scope,
      Map<String, List<MetricSample>> beforeMetrics,
      Map<String, List<MetricSample>> afterMetrics) {
    INVOCATIONS.incrementAndGet();
    if (FAIL_JOB2
        && scope.type() == MetricScope.Type.JOB
        && JobProviderForTest.JOB2.equals(scope.identifier())) {
      return false;
    }
    return true;
  }

  public static void reset() {
    INVOCATIONS.set(0);
    FAIL_JOB2 = false;
  }

  public static void failJob2(boolean failJob2) {
    FAIL_JOB2 = failJob2;
  }
}
