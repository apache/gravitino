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

package org.apache.gravitino.iceberg.service.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestIcebergMetricsFormatter {

  private static MetricsReport createMetricsReport() {
    ImmutableCommitMetricsResult commitMetricsResult =
        ImmutableCommitMetricsResult.builder().build();
    return ImmutableCommitReport.builder()
        .tableName("a")
        .snapshotId(1)
        .sequenceNumber(1)
        .operation("select")
        .commitMetrics(commitMetricsResult)
        .build();
  }

  private static IcebergMetricsFormatter formatter;
  private static MetricsReport report;
  private static final String expectedString =
      "{\"table-name\":\"a\",\"snapshot-id\":1,\"sequence-number\":1,\"operation\":\"select\","
          + "\"commit-metrics\":{\"total-duration\":null,\"attempts\":null,\"added-data-files\":null,"
          + "\"removed-data-files\":null,\"total-data-files\":null,\"added-delete-files\":null,"
          + "\"added-equality-delete-files\":null,\"added-positional-delete-files\":null,"
          + "\"removed-delete-files\":null,\"removed-equality-delete-files\":null,"
          + "\"removed-positional-delete-files\":null,\"total-delete-files\":null,"
          + "\"added-records\":null,\"removed-records\":null,\"total-records\":null,"
          + "\"added-files-size-in-bytes\":null,\"removed-files-size-in-bytes\":null,"
          + "\"total-files-size-in-bytes\":null,\"added-positional-deletes\":null,"
          + "\"removed-positional-deletes\":null,\"total-positional-deletes\":null,"
          + "\"added-equality-deletes\":null,\"removed-equality-deletes\":null,"
          + "\"total-equality-deletes\":null},\"metadata\":{}}";

  @BeforeAll
  static void setup() {
    formatter = new IcebergMetricsFormatter();
    report = createMetricsReport();
  }

  @Test
  void testToPrintableString() throws JsonProcessingException {
    String reportString = formatter.toPrintableString(report);
    Assertions.assertEquals(expectedString, reportString);
  }

  @Test
  void testToJson() {
    IcebergMetricsFormatter formatter = new IcebergMetricsFormatter();
    MetricsReport report = createMetricsReport();
    try {
      String jsonString = formatter.toJson(report);
      Assertions.assertEquals(expectedString, jsonString);
    } catch (JsonProcessingException ex) {
      Assertions.fail("Failed to format metrics report to json");
    }
  }
}
