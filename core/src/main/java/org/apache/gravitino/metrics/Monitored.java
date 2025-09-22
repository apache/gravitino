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
package org.apache.gravitino.metrics;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.gravitino.metrics.source.MetricsSource;

/**
 * An annotation for monitoring method performance. It automatically tracks the execution time,
 * success count, and failure count.
 *
 * <p>The metric names will be generated based on the {@link #metricsSource()} and {@link
 * #baseMetricName()} and the method's outcome. The final metric name will be in the format: {@code
 * {metricsSource}.{baseMetricName}.{suffix}}.
 *
 * <p>The generated metrics like:
 *
 * <ul>
 *   <li>{@code {metricsSource}.{baseMetricName}.total}: A timer for overall execution duration.
 *   <li>{@code {metricsSource}.{baseMetricName}.success}: A meter for successful executions.
 *   <li>{@code {metricsSource}.{baseMetricName}.failure}: A meter for failed executions.
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Monitored {

  /**
   * The {@link MetricsSource} name of the metric belongs to.
   *
   * @return the metrics source name
   */
  String metricsSource();

  /**
   * The base name for the metric.
   *
   * @return the base name for the metric
   */
  String baseMetricName();
}
