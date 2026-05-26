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

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestFlussTableOperations {

  private static final TablePath TABLE_PATH = TablePath.of("db", "orders");
  private static final List<String> PARTITION_KEYS = List.of("event_day", "region");
  private static final String QUALIFIED_PARTITION_NAME = "event_day=20250405/region=US";

  @Test
  void testListPartitionsUsesQualifiedIdentityNames() {
    Admin admin = mock(Admin.class);
    when(admin.listPartitionInfos(TABLE_PATH))
        .thenReturn(CompletableFuture.completedFuture(List.of(partitionInfo())));
    FlussTableOperations operations = operations(admin);

    Partition[] partitions = operations.listPartitions();

    assertEquals(1, partitions.length);
    assertEquals(QUALIFIED_PARTITION_NAME, partitions[0].name());
    assertArrayEquals(new String[] {QUALIFIED_PARTITION_NAME}, operations.listPartitionNames());
  }

  @Test
  void testGetPartitionByFlussQualifiedName() {
    Admin admin = mock(Admin.class);
    PartitionSpec spec =
        new PartitionSpec(ImmutableMap.of("event_day", "20250405", "region", "US"));
    when(admin.listPartitionInfos(TABLE_PATH, spec))
        .thenReturn(CompletableFuture.completedFuture(List.of(partitionInfo())));
    FlussTableOperations operations = operations(admin);

    IdentityPartition partition =
        (IdentityPartition) operations.getPartition(QUALIFIED_PARTITION_NAME);

    assertEquals(QUALIFIED_PARTITION_NAME, partition.name());
    assertEquals("event_day", partition.fieldNames()[0][0]);
    assertEquals("region", partition.fieldNames()[1][0]);
    assertEquals("20250405", partition.values()[0].value());
    assertEquals("US", partition.values()[1].value());
  }

  @Test
  void testDropPartitionByFlussQualifiedName() {
    Admin admin = mock(Admin.class);
    when(admin.dropPartition(eq(TABLE_PATH), any(PartitionSpec.class), eq(false)))
        .thenReturn(CompletableFuture.completedFuture(null));
    FlussTableOperations operations = operations(admin);

    assertTrue(operations.dropPartition(QUALIFIED_PARTITION_NAME));

    ArgumentCaptor<PartitionSpec> specCaptor = ArgumentCaptor.forClass(PartitionSpec.class);
    verify(admin).dropPartition(eq(TABLE_PATH), specCaptor.capture(), eq(false));
    assertEquals("20250405", specCaptor.getValue().getSpecMap().get("event_day"));
    assertEquals("US", specCaptor.getValue().getSpecMap().get("region"));
  }

  @Test
  void testDropPartitionReturnsFalseWhenPartitionDoesNotExist() {
    Admin admin = mock(Admin.class);
    when(admin.dropPartition(eq(TABLE_PATH), any(PartitionSpec.class), eq(false)))
        .thenReturn(failedFuture(new PartitionNotExistException("missing")));
    FlussTableOperations operations = operations(admin);

    assertFalse(operations.dropPartition(QUALIFIED_PARTITION_NAME));
  }

  @Test
  void testAddPartitionCreatesFlussSpecWithConfiguredPartitionKeys() {
    Admin admin = mock(Admin.class);
    when(admin.createPartition(eq(TABLE_PATH), any(PartitionSpec.class), eq(false)))
        .thenReturn(CompletableFuture.completedFuture(null));
    FlussTableOperations operations = operations(admin);

    Partition partition = operations.addPartition(identityPartition());

    assertEquals(QUALIFIED_PARTITION_NAME, partition.name());
    ArgumentCaptor<PartitionSpec> specCaptor = ArgumentCaptor.forClass(PartitionSpec.class);
    verify(admin).createPartition(eq(TABLE_PATH), specCaptor.capture(), eq(false));
    assertEquals("20250405", specCaptor.getValue().getSpecMap().get("event_day"));
    assertEquals("US", specCaptor.getValue().getSpecMap().get("region"));
  }

  @Test
  void testAddPartitionRejectsWrongPartitionKeyOrder() {
    Admin admin = mock(Admin.class);
    FlussTableOperations operations = operations(admin);
    IdentityPartition partition =
        Partitions.identity(
            new String[][] {{"region"}, {"event_day"}},
            new Literal<?>[] {Literals.stringLiteral("US"), Literals.stringLiteral("20250405")});

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> operations.addPartition(partition));

    assertTrue(exception.getMessage().contains("must match Fluss partition key"));
    verifyNoInteractions(admin);
  }

  @Test
  void testGetPartitionRejectsWrongQualifiedPartitionKeyOrder() {
    Admin admin = mock(Admin.class);
    FlussTableOperations operations = operations(admin);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> operations.getPartition("region=US/event_day=20250405"));

    assertTrue(exception.getMessage().contains("must match Fluss partition key"));
    verifyNoInteractions(admin);
  }

  private static FlussTableOperations operations(Admin admin) {
    return new FlussTableOperations(new FlussAdminOps(admin), TABLE_PATH, PARTITION_KEYS);
  }

  private static PartitionInfo partitionInfo() {
    return new PartitionInfo(
        1L, new ResolvedPartitionSpec(PARTITION_KEYS, List.of("20250405", "US")));
  }

  private static IdentityPartition identityPartition() {
    return Partitions.identity(
        new String[][] {{"event_day"}, {"region"}},
        new Literal<?>[] {Literals.stringLiteral("20250405"), Literals.stringLiteral("US")});
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable e) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }
}
