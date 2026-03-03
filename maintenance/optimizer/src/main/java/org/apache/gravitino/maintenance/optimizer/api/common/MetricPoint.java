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

package org.apache.gravitino.maintenance.optimizer.api.common;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.stats.StatisticValue;

/** Immutable metric point for table, partition, or job metrics. */
@DeveloperApi
public final class MetricPoint {

  /** Metric scope. */
  public enum Scope {
    TABLE,
    PARTITION,
    JOB
  }

  private final NameIdentifier identifier;
  private final Scope scope;
  private final Optional<PartitionPath> partitionPath;
  private final String metricName;
  private final StatisticValue<?> value;
  private final long timestampSeconds;

  public MetricPoint(
      NameIdentifier identifier,
      Scope scope,
      Optional<PartitionPath> partitionPath,
      String metricName,
      StatisticValue<?> value,
      long timestampSeconds) {
    Preconditions.checkArgument(identifier != null, "identifier must not be null");
    Preconditions.checkArgument(scope != null, "scope must not be null");
    Preconditions.checkArgument(partitionPath != null, "partitionPath must not be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "metricName must not be blank");
    Preconditions.checkArgument(value != null, "value must not be null");
    Preconditions.checkArgument(timestampSeconds >= 0, "timestampSeconds must be non-negative");
    Preconditions.checkArgument(
        (scope == Scope.PARTITION) == partitionPath.isPresent(),
        "partitionPath must be present only for PARTITION scope");

    this.identifier = identifier;
    this.scope = scope;
    this.partitionPath = partitionPath;
    this.metricName = metricName;
    this.value = value;
    this.timestampSeconds = timestampSeconds;
  }

  public static MetricPoint forTable(
      NameIdentifier identifier,
      String metricName,
      StatisticValue<?> value,
      long timestampSeconds) {
    return new MetricPoint(
        identifier, Scope.TABLE, Optional.empty(), metricName, value, timestampSeconds);
  }

  public static MetricPoint forPartition(
      NameIdentifier identifier,
      PartitionPath partitionPath,
      String metricName,
      StatisticValue<?> value,
      long timestampSeconds) {
    return new MetricPoint(
        identifier,
        Scope.PARTITION,
        Optional.of(partitionPath),
        metricName,
        value,
        timestampSeconds);
  }

  public static MetricPoint forJob(
      NameIdentifier identifier,
      String metricName,
      StatisticValue<?> value,
      long timestampSeconds) {
    return new MetricPoint(
        identifier, Scope.JOB, Optional.empty(), metricName, value, timestampSeconds);
  }

  public NameIdentifier identifier() {
    return identifier;
  }

  public Scope scope() {
    return scope;
  }

  public Optional<PartitionPath> partitionPath() {
    return partitionPath;
  }

  public String metricName() {
    return metricName;
  }

  public StatisticValue<?> value() {
    return value;
  }

  public long timestampSeconds() {
    return timestampSeconds;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MetricPoint)) {
      return false;
    }
    MetricPoint other = (MetricPoint) obj;
    return timestampSeconds == other.timestampSeconds
        && Objects.equals(identifier, other.identifier)
        && scope == other.scope
        && Objects.equals(partitionPath, other.partitionPath)
        && Objects.equals(metricName, other.metricName)
        && Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, scope, partitionPath, metricName, value, timestampSeconds);
  }

  @Override
  public String toString() {
    return "MetricPoint{"
        + "identifier="
        + identifier
        + ", scope="
        + scope
        + ", partitionPath="
        + partitionPath
        + ", metricName='"
        + metricName
        + '\''
        + ", value="
        + value
        + ", timestampSeconds="
        + timestampSeconds
        + '}';
  }
}
