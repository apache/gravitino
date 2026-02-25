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

package org.apache.gravitino.maintenance.optimizer.common.util;

import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator;
import org.apache.gravitino.maintenance.optimizer.monitor.evaluator.MetricsEvaluatorForTest;
import org.apache.gravitino.maintenance.optimizer.updater.StatisticsCalculatorForTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestInstanceLoaderUtils {

  @Test
  public void testCreateMetricsEvaluatorInstance() {
    MetricsEvaluator evaluator =
        InstanceLoaderUtils.createMetricsEvaluatorInstance(MetricsEvaluatorForTest.NAME);
    Assertions.assertNotNull(evaluator);
    Assertions.assertTrue(evaluator instanceof MetricsEvaluatorForTest);
  }

  @Test
  public void testCreateMetricsEvaluatorInstanceWithNullName() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> InstanceLoaderUtils.createMetricsEvaluatorInstance(null));
    Assertions.assertEquals(
        "metrics evaluator name must not be null or blank", exception.getMessage());
  }

  @Test
  public void testCreateMetricsEvaluatorInstanceWithBlankName() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> InstanceLoaderUtils.createMetricsEvaluatorInstance("   "));
    Assertions.assertEquals(
        "metrics evaluator name must not be null or blank", exception.getMessage());
  }

  @Test
  public void testCreateStatisticsCalculatorInstance() {
    StatisticsCalculator calculator =
        InstanceLoaderUtils.createStatisticsCalculatorInstance(StatisticsCalculatorForTest.NAME);
    Assertions.assertNotNull(calculator);
    Assertions.assertTrue(calculator instanceof StatisticsCalculatorForTest);
  }

  @Test
  public void testCreateStatisticsCalculatorInstanceWithUnknownName() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> InstanceLoaderUtils.createStatisticsCalculatorInstance("unknown-calculator"));
    Assertions.assertEquals(
        "No StatisticsCalculator class found for: unknown-calculator", exception.getMessage());
  }
}
