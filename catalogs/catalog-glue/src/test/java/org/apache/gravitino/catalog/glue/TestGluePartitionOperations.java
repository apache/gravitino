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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.gravitino.exceptions.ForbiddenException;
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
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

class TestGluePartitionOperations {

  private GlueClient mockClient;
  private GlueTableOperations ops;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    ops =
        new GlueTableOperations(mockClient, null, "mydb", "mytable", new String[] {"dt", "region"});
    // The table a new partition copies its storage descriptor from.
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(
            GetTableResponse.builder()
                .table(
                    software.amazon.awssdk.services.glue.model.Table.builder()
                        .name("mytable")
                        .storageDescriptor(
                            StorageDescriptor.builder()
                                .location("s3://bucket/mydb/mytable/")
                                .inputFormat(HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS)
                                .outputFormat(HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS)
                                .serdeInfo(
                                    SerDeInfo.builder()
                                        .serializationLibrary(
                                            HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS)
                                        .build())
                                .build())
                        .build())
                .build());
    // Glue returns the partition that was added.
    when(mockClient.getPartition(any(GetPartitionRequest.class)))
        .thenAnswer(
            inv ->
                GetPartitionResponse.builder()
                    .partition(
                        software.amazon.awssdk.services.glue.model.Partition.builder()
                            .values(((GetPartitionRequest) inv.getArgument(0)).partitionValues())
                            .build())
                    .build());
  }

  // -------------------------------------------------------------------------
  // listPartitionNames
  // -------------------------------------------------------------------------

  @Test
  void testListPartitionNames() {
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
  void testListPartitionNamesReturnsEmptyArray() {
    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(GetPartitionsResponse.builder().partitions(List.of()).nextToken(null).build());

    assertEquals(0, ops.listPartitionNames().length);
  }

  // -------------------------------------------------------------------------
  // listPartitions
  // -------------------------------------------------------------------------

  @Test
  void testListPartitions() {
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
  void testGetPartition() {
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
  void testGetPartitionNotFound() {
    when(mockClient.getPartition(any(GetPartitionRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertThrows(NoSuchPartitionException.class, () -> ops.getPartition("dt=2024-01-01/region=us"));
  }

  // -------------------------------------------------------------------------
  // addPartition
  // -------------------------------------------------------------------------

  @Test
  void testAddPartition() {
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
  void testAddPartitionAlreadyExists() {
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
  void testAddPartitionRejectsNullOrEmptyValue() {
    for (org.apache.gravitino.rel.expressions.literals.Literal<?> bad :
        new org.apache.gravitino.rel.expressions.literals.Literal<?>[] {
          Literals.NULL, Literals.stringLiteral("")
        }) {
      IdentityPartition partition =
          Partitions.identity(
              "p",
              new String[][] {{"dt"}, {"region"}},
              new org.apache.gravitino.rel.expressions.literals.Literal[] {
                Literals.stringLiteral("2024-01-01"), bad
              },
              java.util.Collections.emptyMap());

      IllegalArgumentException ex =
          assertThrows(IllegalArgumentException.class, () -> ops.addPartition(partition));
      assertTrue(ex.getMessage().contains("region"), ex.getMessage());
    }
    verify(mockClient, never()).createPartition(any(CreatePartitionRequest.class));
  }

  @Test
  void testAddPartitionValueLengthLimit() {
    IdentityPartition atLimit = partitionWithRegion(StringUtils.repeat('a', 1024));
    ops.addPartition(atLimit);
    verify(mockClient).createPartition(any(CreatePartitionRequest.class));

    IdentityPartition overLimit = partitionWithRegion(StringUtils.repeat('a', 1025));
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ops.addPartition(overLimit));
    assertTrue(ex.getMessage().contains("region"), ex.getMessage());
    verify(mockClient).createPartition(any(CreatePartitionRequest.class));
  }

  private static IdentityPartition partitionWithRegion(String region) {
    return Partitions.identity(
        null,
        new String[][] {{"dt"}, {"region"}},
        new org.apache.gravitino.rel.expressions.literals.Literal[] {
          Literals.stringLiteral("2024-01-01"), Literals.stringLiteral(region)
        },
        java.util.Collections.emptyMap());
  }

  @Test
  void testAddPartitionMapsInvalidInputToIllegalArgument() {
    IdentityPartition partition =
        Partitions.identity(
            "dt=2024-01-01/region=us",
            new String[][] {{"dt"}, {"region"}},
            new org.apache.gravitino.rel.expressions.literals.Literal[] {
              Literals.stringLiteral("2024-01-01"), Literals.stringLiteral("us")
            },
            java.util.Collections.emptyMap());
    when(mockClient.createPartition(any(CreatePartitionRequest.class)))
        .thenThrow(
            InvalidInputException.builder()
                .message("Cannot create partitions in table without partition keys")
                .build());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ops.addPartition(partition));
    assertTrue(ex.getMessage().contains("without partition keys"), ex.getMessage());
  }

  @Test
  void testListPartitionsMapsAccessDeniedToForbidden() {
    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenThrow(
            AccessDeniedException.builder()
                .message("not authorized to perform: glue:GetPartitions")
                .build());

    ForbiddenException ex = assertThrows(ForbiddenException.class, () -> ops.listPartitions());
    assertTrue(ex.getMessage().contains("glue:GetPartitions"), ex.getMessage());
  }

  @Test
  void testAddPartitionRejectsNonIdentity() {
    Partition nonIdentity = mock(Partition.class);
    assertThrows(IllegalArgumentException.class, () -> ops.addPartition(nonIdentity));
  }

  // -------------------------------------------------------------------------
  // dropPartition
  // -------------------------------------------------------------------------

  @Test
  void testDropPartition() {
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
  void testDropPartitionNotFound() {
    when(mockClient.deletePartition(any(DeletePartitionRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertFalse(ops.dropPartition("dt=2024-01-01/region=us"));
  }

  @Test
  void testDropPartitionWithCatalogId() {
    GlueTableOperations opsWithId =
        new GlueTableOperations(mockClient, "123456789012", "mydb", "mytable", new String[] {"dt"});
    ArgumentCaptor<DeletePartitionRequest> captor =
        ArgumentCaptor.forClass(DeletePartitionRequest.class);

    opsWithId.dropPartition("dt=2024-01-01");

    verify(mockClient).deletePartition(captor.capture());
    assertEquals("123456789012", captor.getValue().catalogId());
  }

  private static IdentityPartition partition(String name, String[][] fieldNames, String... values) {
    org.apache.gravitino.rel.expressions.literals.Literal<?>[] literals =
        new org.apache.gravitino.rel.expressions.literals.Literal<?>[values.length];
    for (int i = 0; i < values.length; i++) {
      literals[i] = Literals.stringLiteral(values[i]);
    }
    return Partitions.identity(name, fieldNames, literals, java.util.Collections.emptyMap());
  }

  private PartitionInput addAndCapture(IdentityPartition partition) {
    ops.addPartition(partition);
    ArgumentCaptor<CreatePartitionRequest> captor =
        ArgumentCaptor.forClass(CreatePartitionRequest.class);
    verify(mockClient).createPartition(captor.capture());
    return captor.getValue().partitionInput();
  }

  @Test
  void testPartitionValueWithSlashIsEscapedAndAddressable() {
    PartitionInput input =
        addAndCapture(partition(null, new String[][] {{"dt"}, {"region"}}, "2025/12/31", "us"));
    assertEquals(List.of("2025/12/31", "us"), input.values());

    Partition added = ops.getPartition("dt=2025%2F12%2F31/region=us");
    assertEquals("dt=2025%2F12%2F31/region=us", added.name());

    ArgumentCaptor<GetPartitionRequest> getCaptor =
        ArgumentCaptor.forClass(GetPartitionRequest.class);
    verify(mockClient, org.mockito.Mockito.atLeastOnce()).getPartition(getCaptor.capture());
    assertEquals(List.of("2025/12/31", "us"), getCaptor.getValue().partitionValues());
  }

  @Test
  void testAddPartitionAcceptsUnescapedName() {
    PartitionInput input =
        addAndCapture(
            partition(
                "dt=2025/12/31/region=us",
                new String[][] {{"dt"}, {"region"}},
                "2025/12/31",
                "us"));
    assertEquals(List.of("2025/12/31", "us"), input.values());
  }

  @Test
  void testEscapeRoundTrip() {
    String value = "a/b=c%d:e#f";
    assertEquals(
        value, GlueTableOperations.unescapePathName(GlueTableOperations.escapePathName(value)));
    assertEquals("100%", GlueTableOperations.unescapePathName("100%"));
    assertEquals("%+1", GlueTableOperations.unescapePathName("%+1"));
  }

  @Test
  void testAddPartitionPlacesValuesByFieldName() {
    PartitionInput input =
        addAndCapture(partition(null, new String[][] {{"region"}, {"dt"}}, "us", "2024-01-01"));
    assertEquals(List.of("2024-01-01", "us"), input.values());
  }

  @Test
  void testAddPartitionRefusesUnknownFieldOrMismatchedName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.addPartition(partition(null, new String[][] {{"zz"}, {"yy"}}, "a", "b")));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.addPartition(
                    partition(
                        "dt=q/region=9", new String[][] {{"dt"}, {"region"}}, "2024-01-01", "us")));
    assertTrue(e.getMessage().contains("does not match"), e.getMessage());
    verify(mockClient, never()).createPartition(any(CreatePartitionRequest.class));
  }

  @Test
  void testAddPartitionCopiesTableStorageDescriptor() {
    PartitionInput input =
        addAndCapture(partition(null, new String[][] {{"dt"}, {"region"}}, "2024-01-01", "us"));
    StorageDescriptor sd = input.storageDescriptor();
    assertEquals("s3://bucket/mydb/mytable/dt=2024-01-01/region=us", sd.location());
    assertEquals(HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS, sd.inputFormat());
    assertEquals(
        HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS, sd.serdeInfo().serializationLibrary());
  }

  @Test
  void testAddPartitionRefusesTableWithoutLocation() {
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(
            GetTableResponse.builder()
                .table(
                    software.amazon.awssdk.services.glue.model.Table.builder()
                        .name("mytable")
                        .build())
                .build());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.addPartition(
                    partition(null, new String[][] {{"dt"}, {"region"}}, "2024-01-01", "us")));
    assertTrue(e.getMessage().contains("no storage location"), e.getMessage());
  }

  @Test
  void testAddPartitionNameIgnoresCaseOfColumnNames() {
    PartitionInput input =
        addAndCapture(
            partition(
                "DT=2024-01-01/Region=us",
                new String[][] {{"dt"}, {"region"}},
                "2024-01-01",
                "us"));
    assertEquals(List.of("2024-01-01", "us"), input.values());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.addPartition(
                partition(
                    "dt=2024-01-01/region=US",
                    new String[][] {{"dt"}, {"region"}},
                    "2024-01-01",
                    "us")));
  }

  @Test
  void testAddPartitionReturnsTheRequestedValuesWithoutReadingBack() {
    IdentityPartition partition =
        Partitions.identity(
            null,
            new String[][] {{"region"}, {"dt"}},
            new org.apache.gravitino.rel.expressions.literals.Literal<?>[] {
              Literals.stringLiteral("us"), Literals.dateLiteral(java.time.LocalDate.of(2024, 1, 1))
            },
            Map.of("k", "v"));

    IdentityPartition result = (IdentityPartition) ops.addPartition(partition);

    assertEquals("dt=2024-01-01/region=us", result.name());
    assertArrayEquals(new String[][] {{"dt"}, {"region"}}, result.fieldNames());
    assertEquals(Literals.dateLiteral(java.time.LocalDate.of(2024, 1, 1)), result.values()[0]);
    assertEquals(Map.of("k", "v"), result.properties());
    verify(mockClient, never()).getPartition(any(GetPartitionRequest.class));
  }
}
