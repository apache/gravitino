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

package org.apache.gravitino.maintenance.optimizer.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

/** A complete pair of manifest measurements for one resolved Iceberg partition spec. */
public final class IcebergManifestStatistics {
  /** Object-valued manifest counts keyed by decimal spec ID. */
  public static final String MANIFEST_NUMBER = "custom-manifest-number-by-spec";
  /** Object-valued average manifest sizes in bytes keyed by decimal spec ID. */
  public static final String AVG_MANIFEST_SIZE = "custom-avg-manifest-size-by-spec";

  private final int specId;
  private final long count;
  private final double averageSize;

  /**
   * Creates a complete measurement for a resolved spec.
   *
   * @param specId non-negative resolved spec ID
   * @param count non-negative manifest count
   * @param averageSize finite non-negative average size in bytes, zero for no manifests
   */
  public IcebergManifestStatistics(int specId, long count, double averageSize) {
    if (specId < 0
        || count < 0
        || !Double.isFinite(averageSize)
        || averageSize < 0
        || (count == 0 && averageSize != 0)) {
      throw new IllegalArgumentException("Invalid manifest measurements");
    }
    this.specId = specId;
    this.count = count;
    this.averageSize = averageSize;
  }

  /**
   * Returns the partition spec used for collection.
   *
   * @return the resolved spec ID to retain through evaluation and job submission
   */
  public int specId() {
    return specId;
  }

  /**
   * Returns the collected manifest count.
   *
   * @return manifest count for this spec
   */
  public long count() {
    return count;
  }

  /**
   * Returns the collected average manifest size.
   *
   * @return average manifest size in bytes for this spec
   */
  public double averageSize() {
    return averageSize;
  }

  /**
   * Converts this measurement into a single-spec update for the statistics API.
   *
   * @return both object-valued measurements to publish in one atomic merge
   */
  public List<StatisticEntry<?>> statistics() {
    String key = Integer.toString(specId);
    return Arrays.asList(
        new StatisticEntryImpl<>(
            MANIFEST_NUMBER,
            StatisticValues.objectValue(
                Collections.singletonMap(key, StatisticValues.longValue(count)))),
        new StatisticEntryImpl<>(
            AVG_MANIFEST_SIZE,
            StatisticValues.objectValue(
                Collections.singletonMap(key, StatisticValues.doubleValue(averageSize)))));
  }

  /**
   * Reads a complete pair from one table-statistics response. Callers must not combine responses
   * from separate reads. An absent measurement means collection is required, not zero manifests.
   *
   * @param statistics values from one atomic table-statistics read
   * @param specId the previously resolved spec ID
   * @return a complete measurement, or empty when either spec entry is absent
   */
  public static Optional<IcebergManifestStatistics> fromStatistics(
      Map<String, StatisticValue<?>> statistics, int specId) {
    if (specId < 0) {
      throw new IllegalArgumentException("Spec ID must be non-negative");
    }
    StatisticValue<?> count = entry(statistics.get(MANIFEST_NUMBER), specId);
    StatisticValue<?> average = entry(statistics.get(AVG_MANIFEST_SIZE), specId);
    if (count == null || average == null) {
      return Optional.empty();
    }
    if (!(count instanceof StatisticValues.LongValue)
        || !(average instanceof StatisticValues.DoubleValue)) {
      throw new IllegalArgumentException(
          "Manifest measurements must be a long count and double size");
    }
    return Optional.of(
        new IcebergManifestStatistics(
            specId,
            ((StatisticValues.LongValue) count).value(),
            ((StatisticValues.DoubleValue) average).value()));
  }

  @Nullable
  private static StatisticValue<?> entry(@Nullable StatisticValue<?> value, int specId) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof StatisticValues.ObjectValue)) {
      throw new IllegalArgumentException("Manifest statistics must be object values");
    }
    return ((StatisticValues.ObjectValue) value).value().get(Integer.toString(specId));
  }
}
