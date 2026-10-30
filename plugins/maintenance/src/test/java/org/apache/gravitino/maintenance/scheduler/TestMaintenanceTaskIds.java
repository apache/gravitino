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
package org.apache.gravitino.maintenance.scheduler;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceTaskIds {

  @Test
  void testSeparators() {
    Assertions.assertEquals(':', MaintenanceTaskIds.KV_SEP);
    Assertions.assertEquals(';', MaintenanceTaskIds.PAIR_SEP);
  }

  @Test
  void testExpandPolicyRoundTrip() {
    Assertions.assertEquals("policy:42", MaintenanceTaskIds.forExpandPolicy(42L));
    Assertions.assertEquals(42L, MaintenanceTaskIds.parseExpandPolicy("policy:42").orElseThrow());
  }

  @Test
  void testCronTableRoundTrip() {
    String id = MaintenanceTaskIds.forCronTable(10L, 20L);
    Assertions.assertEquals("table:10;policy:20", id);
    Optional<long[]> parsed = MaintenanceTaskIds.parseCronTable(id);
    Assertions.assertTrue(parsed.isPresent());
    Assertions.assertArrayEquals(new long[] {10L, 20L}, parsed.get());
  }

  @Test
  void testCronBatchRoundTrip() {
    Assertions.assertEquals("batch:0;policy:9", MaintenanceTaskIds.forCronBatch(0L, 9L));
    Assertions.assertEquals("batch:12;policy:9", MaintenanceTaskIds.forCronBatch(12L, 9L));
    Optional<long[]> parsed = MaintenanceTaskIds.parseCronBatch("batch:12;policy:9");
    Assertions.assertTrue(parsed.isPresent());
    Assertions.assertArrayEquals(new long[] {12L, 9L}, parsed.get());
  }

  @Test
  void testCommitTableRoundTrip() {
    Assertions.assertEquals("table:99", MaintenanceTaskIds.forCommitTable(99L));
    Assertions.assertEquals(99L, MaintenanceTaskIds.parseCommitTable("table:99").orElseThrow());
  }

  @Test
  void testParseRejectsInvalid() {
    Assertions.assertTrue(MaintenanceTaskIds.parseExpandPolicy(null).isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseExpandPolicy("42").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseExpandPolicy("policy:").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseExpandPolicy("policy:a").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseExpandPolicy("policy:1:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseExpandPolicy("table:1").isEmpty());

    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable(null).isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable("batch:1;policy:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable("table:").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable("table:1").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable("table:1:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable("table:1:policy:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronTable("table:a;policy:2").isEmpty());

    Assertions.assertTrue(MaintenanceTaskIds.parseCronBatch(null).isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronBatch("table:1;policy:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronBatch("batch:1:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCronBatch("batch:pending;policy:9").isEmpty());

    Assertions.assertTrue(MaintenanceTaskIds.parseCommitTable(null).isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCommitTable("99").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCommitTable("table:").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCommitTable("table:a").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCommitTable("table:1;policy:2").isEmpty());
    Assertions.assertTrue(MaintenanceTaskIds.parseCommitTable("policy:1").isEmpty());
  }
}
