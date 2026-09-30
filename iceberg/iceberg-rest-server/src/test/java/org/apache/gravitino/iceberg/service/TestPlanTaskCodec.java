/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestPlanTaskCodec {

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "tbl");

  @Test
  void testRoundTripPreservesScanRequestAndRange() {
    PlanTableScanRequest scanRequest =
        PlanTableScanRequest.builder()
            .withSnapshotId(42L)
            .withSelect(ImmutableList.of("id", "data"))
            .withFilter(Expressions.greaterThan("id", 5))
            .withCaseSensitive(false)
            .withStatsFields(ImmutableList.of("id"))
            .build();

    PlanTaskCodec.PlanTask planTask =
        PlanTaskCodec.decode(PlanTaskCodec.encode(TABLE, scanRequest, 1000, 500))
            .orElseThrow(() -> new AssertionError("Plan task should decode"));

    Assertions.assertTrue(planTask.matchesTable(TABLE));
    Assertions.assertEquals(1000, planTask.offset());
    Assertions.assertEquals(500, planTask.limit());
    Assertions.assertEquals(42L, planTask.scanRequest().snapshotId());
    Assertions.assertEquals(ImmutableList.of("id", "data"), planTask.scanRequest().select());
    Assertions.assertEquals(
        scanRequest.filter().toString(), planTask.scanRequest().filter().toString());
    Assertions.assertFalse(planTask.scanRequest().caseSensitive());
    Assertions.assertEquals(ImmutableList.of("id"), planTask.scanRequest().statsFields());
  }

  @Test
  void testPlanTaskIsRejectedForAnotherTable() {
    PlanTaskCodec.PlanTask planTask =
        PlanTaskCodec.decode(
                PlanTaskCodec.encode(TABLE, PlanTableScanRequest.builder().build(), 0, 10))
            .orElseThrow(() -> new AssertionError("Plan task should decode"));

    Assertions.assertFalse(planTask.matchesTable(TableIdentifier.of(Namespace.of("db"), "other")));
    Assertions.assertFalse(planTask.matchesTable(TableIdentifier.of(Namespace.of("other"), "tbl")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "unknown-token", "not base64 at all!", "e30", "bm90IGpzb24"})
  void testPlanTasksThisServerDidNotIssueAreNotDecoded(String planTask) {
    Assertions.assertEquals(Optional.empty(), PlanTaskCodec.decode(planTask));
  }

  @Test
  void testNullPlanTaskIsNotDecoded() {
    Assertions.assertEquals(Optional.empty(), PlanTaskCodec.decode(null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Ranges that cannot address any task.
        "{\"table\":\"db.tbl\",\"offset\":-1,\"limit\":10,\"scan\":{}}",
        "{\"table\":\"db.tbl\",\"offset\":0,\"limit\":0,\"scan\":{}}",
        // Missing fields.
        "{\"table\":\"db.tbl\",\"offset\":0,\"limit\":10}",
        "{\"offset\":0,\"limit\":10,\"scan\":{}}",
        "{\"table\":\"db.tbl\",\"limit\":10,\"scan\":{}}",
        // Fields of the wrong type.
        "{\"table\":\"db.tbl\",\"offset\":\"first\",\"limit\":10,\"scan\":{}}",
        "[\"db.tbl\",0,10]"
      })
  void testMalformedPayloadsAreNotDecoded(String payload) {
    String planTask =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));

    Assertions.assertEquals(Optional.empty(), PlanTaskCodec.decode(planTask));
  }

  @Test
  void testPlanTaskIsUrlSafe() {
    String planTask =
        PlanTaskCodec.encode(
            TableIdentifier.of(Namespace.of("db"), "tbl"),
            PlanTableScanRequest.builder().withFilter(Expressions.equal("data", "a/b+c=d")).build(),
            0,
            10);

    Assertions.assertTrue(
        planTask.matches("[A-Za-z0-9_-]+"),
        "Plan tasks travel in JSON bodies and logs, but was: " + planTask);
  }
}
