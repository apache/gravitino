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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;

class TestGlueTableOperations {

  private GlueClient mockClient;
  private GlueTableOperations ops;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    ops =
        new GlueTableOperations(mockClient, null, "mydb", "mytable", new String[] {"dt", "region"});
  }

  // -------------------------------------------------------------------------
  // listPartitionNames
  // -------------------------------------------------------------------------

  @Test
  void testListPartitionNames_paginated() {
    software.amazon.awssdk.services.glue.model.Partition p1 =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("2024-01-01", "us")
            .build();
    software.amazon.awssdk.services.glue.model.Partition p2 =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("2024-01-02", "eu")
            .build();

    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(GetPartitionsResponse.builder().partitions(p1).nextToken("tok").build())
        .thenReturn(GetPartitionsResponse.builder().partitions(p2).nextToken(null).build());

    String[] names = ops.listPartitionNames();

    assertEquals(2, names.length);
    assertEquals("dt=2024-01-01/region=us", names[0]);
    assertEquals("dt=2024-01-02/region=eu", names[1]);
  }

  @Test
  void testListPartitionNames_empty() {
    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(GetPartitionsResponse.builder().partitions(List.of()).nextToken(null).build());

    assertEquals(0, ops.listPartitionNames().length);
  }

  // -------------------------------------------------------------------------
  // listPartitions
  // -------------------------------------------------------------------------

  @Test
  void testListPartitions_success() {
    software.amazon.awssdk.services.glue.model.Partition p =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("2024-01-01", "us")
            .build();

    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(GetPartitionsResponse.builder().partitions(p).nextToken(null).build());

    Partition[] partitions = ops.listPartitions();

    assertEquals(1, partitions.length);
    IdentityPartition ip = (IdentityPartition) partitions[0];
    assertEquals("dt=2024-01-01/region=us", ip.name());
    assertEquals("2024-01-01", ip.values()[0].value());
    assertEquals("us", ip.values()[1].value());
  }

  // -------------------------------------------------------------------------
  // getPartition
  // -------------------------------------------------------------------------

  @Test
  void testGetPartition_success() {
    software.amazon.awssdk.services.glue.model.Partition gluePartition =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("2024-01-01", "us")
            .build();

    ArgumentCaptor<GetPartitionRequest> captor = ArgumentCaptor.forClass(GetPartitionRequest.class);
    when(mockClient.getPartition(any(GetPartitionRequest.class)))
        .thenReturn(GetPartitionResponse.builder().partition(gluePartition).build());

    Partition result = ops.getPartition("dt=2024-01-01/region=us");

    verify(mockClient).getPartition(captor.capture());
    assertEquals(List.of("2024-01-01", "us"), captor.getValue().partitionValues());
    assertEquals("dt=2024-01-01/region=us", result.name());
  }

  @Test
  void testGetPartition_notFound() {
    when(mockClient.getPartition(any(GetPartitionRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(NoSuchPartitionException.class, () -> ops.getPartition("dt=2024-01-01/region=us"));
  }

  // -------------------------------------------------------------------------
  // addPartition
  // -------------------------------------------------------------------------

  @Test
  void testAddPartition_success() {
    IdentityPartition partition =
        Partitions.identity(
            "dt=2024-01-01/region=us",
            new String[][] {{"dt"}, {"region"}},
            new org.apache.gravitino.rel.expressions.literals.Literal[] {
              Literals.stringLiteral("2024-01-01"), Literals.stringLiteral("us")
            },
            java.util.Collections.emptyMap());

    ArgumentCaptor<CreatePartitionRequest> captor =
        ArgumentCaptor.forClass(CreatePartitionRequest.class);

    Partition result = ops.addPartition(partition);

    verify(mockClient).createPartition(captor.capture());
    assertEquals(List.of("2024-01-01", "us"), captor.getValue().partitionInput().values());
    assertEquals("dt=2024-01-01/region=us", result.name());
  }

  @Test
  void testAddPartition_alreadyExists() {
    IdentityPartition partition =
        Partitions.identity(
            "dt=2024-01-01/region=us",
            new String[][] {{"dt"}, {"region"}},
            new org.apache.gravitino.rel.expressions.literals.Literal[] {
              Literals.stringLiteral("2024-01-01"), Literals.stringLiteral("us")
            },
            java.util.Collections.emptyMap());

    when(mockClient.createPartition(any(CreatePartitionRequest.class)))
        .thenThrow(AlreadyExistsException.builder().message("exists").build());

    assertThrows(PartitionAlreadyExistsException.class, () -> ops.addPartition(partition));
  }

  @Test
  void testAddPartition_nonIdentityRejected() {
    Partition nonIdentity = mock(Partition.class);
    assertThrows(IllegalArgumentException.class, () -> ops.addPartition(nonIdentity));
  }

  // -------------------------------------------------------------------------
  // dropPartition
  // -------------------------------------------------------------------------

  @Test
  void testDropPartition_success() {
    ArgumentCaptor<DeletePartitionRequest> captor =
        ArgumentCaptor.forClass(DeletePartitionRequest.class);

    boolean result = ops.dropPartition("dt=2024-01-01/region=us");

    verify(mockClient).deletePartition(captor.capture());
    assertTrue(result);
    assertEquals(List.of("2024-01-01", "us"), captor.getValue().partitionValues());
    assertEquals("mydb", captor.getValue().databaseName());
    assertEquals("mytable", captor.getValue().tableName());
  }

  @Test
  void testDropPartition_notFound() {
    when(mockClient.deletePartition(any(DeletePartitionRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertFalse(ops.dropPartition("dt=2024-01-01/region=us"));
  }

  @Test
  void testDropPartition_withCatalogId() {
    GlueTableOperations opsWithId =
        new GlueTableOperations(mockClient, "123456789012", "mydb", "mytable", new String[] {"dt"});
    ArgumentCaptor<DeletePartitionRequest> captor =
        ArgumentCaptor.forClass(DeletePartitionRequest.class);

    opsWithId.dropPartition("dt=2024-01-01");

    verify(mockClient).deletePartition(captor.capture());
    assertEquals("123456789012", captor.getValue().catalogId());
  }
}
