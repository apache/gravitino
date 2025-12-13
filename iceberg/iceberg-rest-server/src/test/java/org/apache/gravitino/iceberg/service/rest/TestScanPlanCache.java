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

package org.apache.gravitino.iceberg.service.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.iceberg.service.cache.LocalScanPlanCache;
import org.apache.gravitino.iceberg.service.cache.ScanPlanCache;
import org.apache.gravitino.iceberg.service.cache.ScanPlanCacheKey;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestScanPlanCache {

  private ScanPlanCache scanPlanCache;
  private Table mockTable;
  private Snapshot mockSnapshot;
  private TableIdentifier tableIdentifier;

  @BeforeEach
  public void setUp() {
    scanPlanCache = new LocalScanPlanCache();
    scanPlanCache.initialize(10, 60);
    tableIdentifier = TableIdentifier.of(Namespace.of("test_db"), "test_table");

    mockSnapshot = mock(Snapshot.class);
    when(mockSnapshot.snapshotId()).thenReturn(1L);

    mockTable = mock(Table.class);
    when(mockTable.name()).thenReturn("test_table");
    when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scanPlanCache != null) {
      scanPlanCache.close();
    }
  }

  @Test
  public void testCacheHit() {
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(1L).build();
    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task1"))
            .build();

    Optional<PlanTableScanResponse> cached1 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, scanRequest));
    Assertions.assertFalse(cached1.isPresent(), "First call should be cache miss");

    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, scanRequest), response);

    Optional<PlanTableScanResponse> cached2 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, scanRequest));
    Assertions.assertTrue(cached2.isPresent(), "Second call should be cache hit");
    Assertions.assertEquals(response, cached2.get());
    Assertions.assertEquals(PlanStatus.COMPLETED, cached2.get().planStatus());
  }

  @Test
  public void testCacheMiss() {
    PlanTableScanRequest request1 = new PlanTableScanRequest.Builder().withSnapshotId(1L).build();
    PlanTableScanRequest request2 =
        new PlanTableScanRequest.Builder()
            .withSelect(Arrays.asList("id", "name"))
            .withSnapshotId(1L)
            .build();

    PlanTableScanResponse response1 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task1"))
            .build();

    PlanTableScanResponse response2 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task2"))
            .build();

    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1), response1);
    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, request2), response2);

    Optional<PlanTableScanResponse> cached1 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1));
    Assertions.assertTrue(cached1.isPresent());
    Assertions.assertEquals(response1, cached1.get());

    Optional<PlanTableScanResponse> cached2 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request2));
    Assertions.assertTrue(cached2.isPresent());
    Assertions.assertEquals(response2, cached2.get());
    Assertions.assertNotEquals(
        cached1.get(), cached2.get(), "Different requests should have different cache entries");
  }

  @Test
  public void testCacheKeyWithDifferentSelectOrder() {
    PlanTableScanRequest request1 =
        new PlanTableScanRequest.Builder()
            .withSelect(Arrays.asList("id", "name"))
            .withSnapshotId(1L)
            .build();
    PlanTableScanRequest request2 =
        new PlanTableScanRequest.Builder()
            .withSelect(Arrays.asList("name", "id"))
            .withSnapshotId(1L)
            .build();

    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task1"))
            .build();

    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1), response);

    Optional<PlanTableScanResponse> cached2 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request2));
    Assertions.assertTrue(
        cached2.isPresent(), "Select fields with different order should use same cache key");
    Assertions.assertEquals(response, cached2.get());
  }

  @Test
  public void testCacheKeyWithDifferentTableIdentifier() {
    TableIdentifier table1 = TableIdentifier.of(Namespace.of("db1"), "table1");
    TableIdentifier table2 = TableIdentifier.of(Namespace.of("db2"), "table2");

    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(1L).build();

    PlanTableScanResponse response1 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task1"))
            .build();

    PlanTableScanResponse response2 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task2"))
            .build();

    scanPlanCache.put(ScanPlanCacheKey.create(table1, mockTable, scanRequest), response1);
    scanPlanCache.put(ScanPlanCacheKey.create(table2, mockTable, scanRequest), response2);

    Optional<PlanTableScanResponse> cached1 =
        scanPlanCache.get(ScanPlanCacheKey.create(table1, mockTable, scanRequest));
    Assertions.assertTrue(cached1.isPresent());
    Assertions.assertEquals(response1, cached1.get());

    Optional<PlanTableScanResponse> cached2 =
        scanPlanCache.get(ScanPlanCacheKey.create(table2, mockTable, scanRequest));
    Assertions.assertTrue(cached2.isPresent());
    Assertions.assertEquals(response2, cached2.get());
    Assertions.assertNotEquals(cached1.get(), cached2.get());
  }

  @Test
  public void testCacheKeyWithDifferentSnapshotId() {
    PlanTableScanRequest request1 = new PlanTableScanRequest.Builder().withSnapshotId(1L).build();
    PlanTableScanRequest request2 = new PlanTableScanRequest.Builder().withSnapshotId(2L).build();

    PlanTableScanResponse response1 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task1"))
            .build();

    // Cache with snapshot ID 1
    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1), response1);

    // Query with snapshot ID 2 should result in cache miss
    Optional<PlanTableScanResponse> cached =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request2));
    Assertions.assertFalse(cached.isPresent(), "Different snapshot should result in cache miss");

    // Query with snapshot ID 1 should result in cache hit
    Optional<PlanTableScanResponse> cachedHit =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1));
    Assertions.assertTrue(cachedHit.isPresent(), "Same snapshot should result in cache hit");
    Assertions.assertEquals(response1, cachedHit.get());
  }

  @Test
  public void testCacheKeyWithNullSnapshot() {
    // Test edge case: table.currentSnapshot() returns null (empty table with no snapshots)
    // This test verifies that:
    // 1. Cache works correctly with null snapshots
    // 2. Different requests produce different cache keys even with null snapshot
    // Test that different requests with null snapshot produce different cache keys
    // if they differ in other parameters
    when(mockTable.currentSnapshot()).thenReturn(null);

    PlanTableScanRequest request1 = new PlanTableScanRequest.Builder().withSnapshotId(1L).build();
    PlanTableScanRequest request2 =
        new PlanTableScanRequest.Builder()
            .withSnapshotId(1L)
            .withSelect(Arrays.asList("id", "name"))
            .build();

    PlanTableScanResponse response1 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task1"))
            .build();

    PlanTableScanResponse response2 =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanTasks(Collections.singletonList("task2"))
            .build();

    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1), response1);
    scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, mockTable, request2), response2);

    Optional<PlanTableScanResponse> cached1 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request1));
    Assertions.assertTrue(cached1.isPresent());
    Assertions.assertEquals(response1, cached1.get());

    Optional<PlanTableScanResponse> cached2 =
        scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, mockTable, request2));
    Assertions.assertTrue(cached2.isPresent());
    Assertions.assertEquals(response2, cached2.get());

    Assertions.assertNotEquals(
        cached1.get(), cached2.get(), "Different requests should have different cache entries");
  }
}
