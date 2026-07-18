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

import static org.apache.gravitino.catalog.glue.GlueConstants.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_TYPE_PARAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

class TestGlueCatalogOperationsForIceberg {

  private static final String DB = "mydb";
  private static final String TABLE = "ice1";
  private static final String LOCATION = "s3://my-bucket/warehouse/ice1";

  private GlueClient mockClient;
  private GlueCatalogOperations ops;
  private Catalog mockIcebergCatalog;

  @BeforeEach
  void setup() {
    mockClient = mock(GlueClient.class);
    mockIcebergCatalog = mock(Catalog.class);
    ops = new GlueCatalogOperations();
    ops.glueClient = mockClient;
    ops.catalogId = null;
    ops.tableFormatFilter = null;
    ops.defaultTableFormat = GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
    ops.icebergGlueCatalog = mockIcebergCatalog;
  }

  // ---------------------------------------------------------------------------
  // isIcebergTable
  // ---------------------------------------------------------------------------

  @Test
  void testIsIcebergTable_withIcebergType() {
    software.amazon.awssdk.services.glue.model.Table t =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .build();
    assertTrue(GlueIcebergTableHelper.isIcebergTable(t));
  }

  @Test
  void testIsIcebergTable_caseInsensitive() {
    software.amazon.awssdk.services.glue.model.Table t =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .parameters(Map.of("table_type", "iceberg"))
            .build();
    assertTrue(GlueIcebergTableHelper.isIcebergTable(t));
  }

  @Test
  void testIsIcebergTable_hiveTable() {
    software.amazon.awssdk.services.glue.model.Table t =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .parameters(Map.of("table_type", "HIVE"))
            .build();
    assertFalse(GlueIcebergTableHelper.isIcebergTable(t));
  }

  @Test
  void testIsIcebergTable_noParameters() {
    software.amazon.awssdk.services.glue.model.Table t =
        software.amazon.awssdk.services.glue.model.Table.builder().build();
    assertFalse(GlueIcebergTableHelper.isIcebergTable(t));
  }

  // ---------------------------------------------------------------------------
  // createTable Iceberg routing
  // ---------------------------------------------------------------------------

  @Test
  void testCreateTable_icebergRoutesToIcebergSdk() {
    software.amazon.awssdk.services.glue.model.Table created =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(
                Map.of(
                    "table_type",
                    "ICEBERG",
                    "metadata_location",
                    LOCATION + "/metadata/00000.metadata.json"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    Catalog.TableBuilder mockBuilder = mock(Catalog.TableBuilder.class, Mockito.RETURNS_SELF);
    when(mockIcebergCatalog.buildTable(any(TableIdentifier.class), any(Schema.class)))
        .thenReturn(mockBuilder);
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(created).build());

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    GlueColumn[] cols = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };

    ops.createTable(
        ident,
        cols,
        "iceberg table",
        Map.of(GlueConstants.TABLE_FORMAT, "iceberg", GlueConstants.LOCATION, LOCATION),
        new Transform[0],
        Distributions.NONE,
        null,
        Indexes.EMPTY_INDEXES);

    verify(mockIcebergCatalog).buildTable(any(TableIdentifier.class), any(Schema.class));
    verify(mockBuilder).create();
  }

  @Test
  void testCreateTable_icebergRejectsNanosecondTimestampBeforeSdkCall() {
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, "timestamp_ns");
    GlueColumn[] cols = {
      GlueColumn.builder()
          .withName("event_time")
          .withType(Types.TimestampType.withTimeZone(9))
          .withNullable(true)
          .build()
    };

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                ident,
                cols,
                null,
                Map.of(GlueConstants.TABLE_FORMAT, "iceberg", GlueConstants.LOCATION, LOCATION),
                new Transform[0],
                Distributions.NONE,
                null,
                Indexes.EMPTY_INDEXES));

    verify(mockIcebergCatalog, never()).buildTable(any(TableIdentifier.class), any(Schema.class));
  }

  // ---------------------------------------------------------------------------
  // alterTable Iceberg routing
  // ---------------------------------------------------------------------------

  @Test
  void testAlterTable_icebergRoutesToIcebergSdk() {
    software.amazon.awssdk.services.glue.model.Table rawTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build());

    Table mockTable = mock(Table.class);
    org.apache.iceberg.UpdateSchema mockUpdateSchema = mock(org.apache.iceberg.UpdateSchema.class);
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class))).thenReturn(mockTable);
    when(mockTable.updateSchema()).thenReturn(mockUpdateSchema);

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    ops.alterTable(
        ident, TableChange.addColumn(new String[] {"score"}, Types.DoubleType.get(), true));

    verify(mockIcebergCatalog, Mockito.times(2)).loadTable(any(TableIdentifier.class));
    verify(mockUpdateSchema)
        .addColumn("score", org.apache.iceberg.types.Types.DoubleType.get(), (String) null);
    verify(mockUpdateSchema).commit();
  }

  @Test
  void testCreateTable_icebergMissingLocationThrows() {
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    GlueColumn[] cols = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                ident,
                cols,
                "iceberg table",
                Map.of(GlueConstants.TABLE_FORMAT, "iceberg"), // missing location
                new Transform[0],
                Distributions.NONE,
                null,
                Indexes.EMPTY_INDEXES));
  }

  @Test
  void testLoadTableWithIcebergMetadataLoadFailure() {
    software.amazon.awssdk.services.glue.model.Table rawTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build());
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class)))
        .thenThrow(new RuntimeException("metadata file is missing"));
    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(GetPartitionsResponse.builder().build());

    GlueTable result = ops.loadTable(NameIdentifier.of("cat", "ns", DB, TABLE));
    SupportsPartitions partitions = result.supportPartitions();

    assertEquals(0, partitions.listPartitionNames().length);
    ArgumentCaptor<GetPartitionsRequest> captor =
        ArgumentCaptor.forClass(GetPartitionsRequest.class);
    verify(mockClient).getPartitions(captor.capture());
    assertEquals(DB, captor.getValue().databaseName());
    assertEquals(TABLE, captor.getValue().tableName());
  }

  @Test
  void testAlterIcebergTableRename() {
    String newName = "ice1_renamed";
    software.amazon.awssdk.services.glue.model.Table rawTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    software.amazon.awssdk.services.glue.model.Table renamedTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(newName)
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build())
        .thenReturn(GetTableResponse.builder().table(renamedTable).build());
    when(mockIcebergCatalog.loadTable(TableIdentifier.of(DB, newName)))
        .thenThrow(new RuntimeException("no iceberg metadata"));
    when(mockClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(GetPartitionsResponse.builder().build());

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    GlueTable result = ops.alterTable(ident, TableChange.rename(newName));

    ArgumentCaptor<TableIdentifier> fromCaptor = ArgumentCaptor.forClass(TableIdentifier.class);
    ArgumentCaptor<TableIdentifier> toCaptor = ArgumentCaptor.forClass(TableIdentifier.class);
    verify(mockIcebergCatalog).renameTable(fromCaptor.capture(), toCaptor.capture());
    assertEquals(TableIdentifier.of(DB, TABLE), fromCaptor.getValue());
    assertEquals(TableIdentifier.of(DB, newName), toCaptor.getValue());
    assertEquals(newName, result.name());
  }

  @Test
  void testCreateTable_defaultTableFormatIcebergRoutesToIcebergSdk() {
    software.amazon.awssdk.services.glue.model.Table created =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(
                Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE, "metadata_location", LOCATION))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    Catalog.TableBuilder mockBuilder = mock(Catalog.TableBuilder.class, Mockito.RETURNS_SELF);
    when(mockIcebergCatalog.buildTable(any(TableIdentifier.class), any(Schema.class)))
        .thenReturn(mockBuilder);
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(created).build());

    ops.defaultTableFormat = "iceberg";
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    GlueColumn[] cols = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };
    ops.createTable(
        ident,
        cols,
        "iceberg table",
        Map.of(GlueConstants.LOCATION, LOCATION), // no table-format; relies on defaultTableFormat
        new Transform[0],
        Distributions.NONE,
        null,
        Indexes.EMPTY_INDEXES);

    verify(mockIcebergCatalog).buildTable(any(TableIdentifier.class), any(Schema.class));
    verify(mockBuilder).create();
  }

  @Test
  void testMatchesFormatFilter_icebergFallbackViaTableType() {
    // Table has table_type=ICEBERG but no table-format property (e.g. created by external tooling)
    software.amazon.awssdk.services.glue.model.Table externalIceberg =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .build();

    ops.tableFormatFilter = Set.of("iceberg");
    assertTrue(
        GlueIcebergTableHelper.isIcebergTable(externalIceberg),
        "Table with table_type=ICEBERG should be recognized as Iceberg");

    // A hive table should not match iceberg filter
    software.amazon.awssdk.services.glue.model.Table hiveTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name("hive_t")
            .parameters(Map.of("table_type", "HIVE"))
            .build();
    assertFalse(GlueIcebergTableHelper.isIcebergTable(hiveTable));
  }
}
