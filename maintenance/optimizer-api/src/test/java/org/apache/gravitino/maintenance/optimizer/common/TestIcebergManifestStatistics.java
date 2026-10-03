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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergManifestStatistics {
  @Test
  void testMissingAndEmptyMeasurements() {
    Map<String, StatisticValue<?>> values = new HashMap<>();
    Assertions.assertFalse(IcebergManifestStatistics.fromStatistics(values, 2).isPresent());
    new IcebergManifestStatistics(2, 0L, 0D)
        .statistics()
        .forEach(stat -> values.put(stat.name(), stat.value()));
    IcebergManifestStatistics empty = IcebergManifestStatistics.fromStatistics(values, 2).get();
    Assertions.assertEquals(2, empty.specId());
    Assertions.assertEquals(0L, empty.count());
    Assertions.assertEquals(0D, empty.averageSize());
    Assertions.assertFalse(IcebergManifestStatistics.fromStatistics(values, 1).isPresent());
    values.remove(IcebergManifestStatistics.AVG_MANIFEST_SIZE);
    Assertions.assertFalse(IcebergManifestStatistics.fromStatistics(values, 2).isPresent());
    values.put(
        IcebergManifestStatistics.AVG_MANIFEST_SIZE,
        StatisticValues.objectValue(
            Collections.singletonMap("1", StatisticValues.doubleValue(10D))));
    Assertions.assertFalse(IcebergManifestStatistics.fromStatistics(values, 2).isPresent());
  }

  @Test
  void testInvalidMeasurements() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IcebergManifestStatistics(-1, 1, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IcebergManifestStatistics(1, -1, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IcebergManifestStatistics(1, 0, 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new IcebergManifestStatistics(1, 1, Double.NaN));
    Map<String, StatisticValue<?>> values = new HashMap<>();
    new IcebergManifestStatistics(1, 3L, 20D)
        .statistics()
        .forEach(stat -> values.put(stat.name(), stat.value()));
    Assertions.assertEquals(3L, IcebergManifestStatistics.fromStatistics(values, 1).get().count());
    values.put(
        IcebergManifestStatistics.MANIFEST_NUMBER,
        StatisticValues.objectValue(
            Collections.singletonMap("1", StatisticValues.doubleValue(3D))));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IcebergManifestStatistics.fromStatistics(values, 1));
    values.put(IcebergManifestStatistics.MANIFEST_NUMBER, StatisticValues.longValue(3));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IcebergManifestStatistics.fromStatistics(values, 1));
  }
}
