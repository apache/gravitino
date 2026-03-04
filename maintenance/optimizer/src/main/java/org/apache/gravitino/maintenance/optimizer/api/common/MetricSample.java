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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.stats.StatisticValue;

/** Immutable metric sample that keeps only timestamp and value for evaluation. */
@DeveloperApi
public final class MetricSample {

  private final long timestampSeconds;
  private final StatisticValue<?> value;

  public MetricSample(long timestampSeconds, StatisticValue<?> value) {
    Preconditions.checkArgument(timestampSeconds >= 0, "timestampSeconds must be non-negative");
    Preconditions.checkArgument(value != null, "value must not be null");
    this.timestampSeconds = timestampSeconds;
    this.value = value;
  }

  public long timestampSeconds() {
    return timestampSeconds;
  }

  public StatisticValue<?> value() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MetricSample)) {
      return false;
    }
    MetricSample other = (MetricSample) obj;
    return timestampSeconds == other.timestampSeconds && Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampSeconds, value);
  }

  @Override
  public String toString() {
    return "MetricSample{" + "timestampSeconds=" + timestampSeconds + ", value=" + value + '}';
  }
}
