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

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;

/** Validator for checking whether one metric point belongs to a target metric scope. */
public final class MetricScopePointValidator {

  private MetricScopePointValidator() {}

  /**
   * Validate whether one metric point belongs to the provided scope.
   *
   * @param scope expected scope
   * @param metricPoint metric point to validate
   * @return empty if valid; otherwise one invalid reason
   */
  public static Optional<String> invalidReason(MetricScope scope, MetricPoint metricPoint) {
    Preconditions.checkArgument(scope != null, "scope must not be null");
    if (metricPoint == null) {
      return Optional.of("metric point is null");
    }

    if (!scope.identifier().equals(metricPoint.identifier())) {
      return Optional.of("identifier mismatch");
    }

    switch (scope.type()) {
      case TABLE:
        if (metricPoint.scope() != MetricPoint.Scope.TABLE) {
          return Optional.of("scope mismatch");
        }
        if (metricPoint.partitionPath().isPresent()) {
          return Optional.of("table metric must not include partition path");
        }
        return Optional.empty();
      case PARTITION:
        if (metricPoint.scope() != MetricPoint.Scope.PARTITION) {
          return Optional.of("scope mismatch");
        }
        if (!scope.partition().isPresent()) {
          return Optional.of("scope partition missing");
        }
        if (!metricPoint.partitionPath().isPresent()) {
          return Optional.of("metric partition missing");
        }
        if (!scope.partition().get().equals(metricPoint.partitionPath().get())) {
          return Optional.of("partition path mismatch");
        }
        return Optional.empty();
      case JOB:
        if (metricPoint.scope() != MetricPoint.Scope.JOB) {
          return Optional.of("scope mismatch");
        }
        if (metricPoint.partitionPath().isPresent()) {
          return Optional.of("job metric must not include partition path");
        }
        return Optional.empty();
      default:
        return Optional.of("unsupported scope type");
    }
  }
}
