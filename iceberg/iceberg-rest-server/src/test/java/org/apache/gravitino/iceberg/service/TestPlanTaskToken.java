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

public class TestPlanTaskToken {

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

    PlanTaskToken token =
        PlanTaskToken.decode(PlanTaskToken.encode(TABLE, scanRequest, 1000, 500))
            .orElseThrow(() -> new AssertionError("Token should decode"));

    Assertions.assertTrue(token.matchesTable(TABLE));
    Assertions.assertEquals(1000, token.offset());
    Assertions.assertEquals(500, token.limit());
    Assertions.assertEquals(42L, token.scanRequest().snapshotId());
    Assertions.assertEquals(ImmutableList.of("id", "data"), token.scanRequest().select());
    Assertions.assertEquals(
        scanRequest.filter().toString(), token.scanRequest().filter().toString());
    Assertions.assertFalse(token.scanRequest().caseSensitive());
    Assertions.assertEquals(ImmutableList.of("id"), token.scanRequest().statsFields());
  }

  @Test
  void testTokenIsRejectedForAnotherTable() {
    PlanTaskToken token =
        PlanTaskToken.decode(
                PlanTaskToken.encode(TABLE, PlanTableScanRequest.builder().build(), 0, 10))
            .orElseThrow(() -> new AssertionError("Token should decode"));

    Assertions.assertFalse(token.matchesTable(TableIdentifier.of(Namespace.of("db"), "other")));
    Assertions.assertFalse(token.matchesTable(TableIdentifier.of(Namespace.of("other"), "tbl")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "unknown-token", "not base64 at all!", "e30", "bm90IGpzb24"})
  void testTokensThisServerDidNotIssueAreNotDecoded(String planTask) {
    Assertions.assertEquals(Optional.empty(), PlanTaskToken.decode(planTask));
  }

  @Test
  void testNullTokenIsNotDecoded() {
    Assertions.assertEquals(Optional.empty(), PlanTaskToken.decode(null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Unsupported token version, e.g. a token minted by a future server.
        "{\"version\":2,\"table\":\"db.tbl\",\"offset\":0,\"limit\":10,\"scan\":{}}",
        // Ranges that cannot address any task.
        "{\"version\":1,\"table\":\"db.tbl\",\"offset\":-1,\"limit\":10,\"scan\":{}}",
        "{\"version\":1,\"table\":\"db.tbl\",\"offset\":0,\"limit\":0,\"scan\":{}}",
        // Missing fields.
        "{\"version\":1,\"table\":\"db.tbl\",\"offset\":0,\"limit\":10}",
        "{\"version\":1,\"offset\":0,\"limit\":10,\"scan\":{}}"
      })
  void testMalformedTokenPayloadsAreNotDecoded(String payload) {
    String planTask =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));

    Assertions.assertEquals(Optional.empty(), PlanTaskToken.decode(planTask));
  }

  @Test
  void testTokenIsUrlSafe() {
    String planTask =
        PlanTaskToken.encode(
            TableIdentifier.of(Namespace.of("db"), "tbl"),
            PlanTableScanRequest.builder().withFilter(Expressions.equal("data", "a/b+c=d")).build(),
            0,
            10);

    Assertions.assertTrue(
        planTask.matches("[A-Za-z0-9_-]+"),
        "Plan task tokens travel in JSON bodies and logs, but was: " + planTask);
  }
}
