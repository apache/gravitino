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

  @BeforeAll
  static void setup() {
    formatter = new IcebergMetricsFormatter();
    report = createMetricsReport();
  }

  private void validateResult(String result) {
    Assertions.assertTrue(result.contains("\"table-name\":\"a\""));
    Assertions.assertTrue(result.contains("\"snapshot-id\":1"));
    Assertions.assertTrue(result.contains("\"sequence-number\":1"));
    Assertions.assertTrue(result.contains("\"operation\":\"select\""));
  }

  @Test
  void testToPrintableString() throws JsonProcessingException {
    String reportString = formatter.toPrintableString(report);
    validateResult(reportString);
  }

  @Test
  void testToJson() {
    IcebergMetricsFormatter formatter = new IcebergMetricsFormatter();
    MetricsReport report = createMetricsReport();
    try {
      String jsonString = formatter.toJson(report);
      validateResult(jsonString);
    } catch (JsonProcessingException ex) {
      Assertions.fail("Failed to format metrics report to json");
    }
  }
}
