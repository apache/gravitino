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
import static org.apache.gravitino.catalog.glue.GlueConstants.METADATA_LOCATION;
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
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
    stubIcebergTable();

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

    Table mockTable = stubIcebergTable();
    Transaction mockTransaction = mock(Transaction.class);
    org.apache.iceberg.UpdateSchema mockUpdateSchema = mock(org.apache.iceberg.UpdateSchema.class);
    when(mockTable.newTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);
    when(mockUpdateSchema.caseSensitive(false)).thenReturn(mockUpdateSchema);

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    ops.alterTable(
        ident, TableChange.addColumn(new String[] {"score"}, Types.DoubleType.get(), true));

    verify(mockUpdateSchema).caseSensitive(false);

    verify(mockIcebergCatalog, Mockito.times(2)).loadTable(any(TableIdentifier.class));
    verify(mockUpdateSchema)
        .addColumn("score", org.apache.iceberg.types.Types.DoubleType.get(), (String) null);
    verify(mockUpdateSchema).commit();
    verify(mockTransaction).commitTransaction();
  }

  @Test
  void testCreateTable_icebergMissingLocationThrows() {
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    GlueColumn[] cols = {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };
    // The database declares no location either, so there is nothing to derive the location from.
    when(mockClient.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(
            GetDatabaseResponse.builder().database(Database.builder().name(DB).build()).build());

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
  void testLoadTableFailsWhenIcebergMetadataFileIsMissing() {
    String metadataLocation = LOCATION + "/metadata/00001.metadata.json";
    stubGlueTable(
        Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE, METADATA_LOCATION, metadataLocation));
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class)))
        .thenThrow(
            new org.apache.iceberg.exceptions.NotFoundException(
                "Location does not exist: %s", metadataLocation));

    NotFoundException e =
        assertThrows(
            NotFoundException.class,
            () -> ops.loadTable(NameIdentifier.of("cat", "ns", DB, TABLE)));
    assertTrue(e.getMessage().contains(metadataLocation), e.getMessage());
  }

  @Test
  void testLoadTableFailsWhenS3DeniesIcebergMetadata() {
    stubGlueTable(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE));
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class)))
        .thenThrow(
            S3Exception.builder()
                .statusCode(403)
                .awsErrorDetails(
                    AwsErrorDetails.builder()
                        .errorCode("AccessDenied")
                        .errorMessage("Access Denied")
                        .build())
                .build());

    ForbiddenException e =
        assertThrows(
            ForbiddenException.class,
            () -> ops.loadTable(NameIdentifier.of("cat", "ns", DB, TABLE)));
    assertTrue(e.getMessage().contains("S3 denied access"), e.getMessage());
    assertTrue(e.getMessage().contains("Iceberg metadata of table " + TABLE), e.getMessage());
    assertFalse(e.getMessage().contains("Glue error"), e.getMessage());
  }

  @Test
  void testPartitionOperationsRefusedForIcebergTable() {
    stubGlueTable(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE));
    stubIcebergTable();

    GlueTable table = ops.loadTable(NameIdentifier.of("cat", "ns", DB, TABLE));

    assertThrows(UnsupportedOperationException.class, table::supportPartitions);
    verify(mockClient, never()).getPartitions(any(GetPartitionsRequest.class));
  }

  @Test
  void testRegisterIcebergTableUsesMetadataFileAndMarksTable() {
    String metadataLocation = LOCATION + "/metadata/00001.metadata.json";
    Table registered = mockIcebergTable();
    when(mockIcebergCatalog.registerTable(TableIdentifier.of(DB, TABLE), metadataLocation))
        .thenReturn(registered);
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class))).thenReturn(registered);
    stubGlueTable(
        Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE, METADATA_LOCATION, metadataLocation));

    ops.createTable(
        NameIdentifier.of("cat", "ns", DB, TABLE),
        icebergColumns(),
        "registered",
        Map.of(
            GlueConstants.TABLE_FORMAT, "iceberg", METADATA_LOCATION, metadataLocation, "k", "v"),
        new Transform[0],
        Distributions.NONE,
        null,
        Indexes.EMPTY_INDEXES);

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    Map<String, String> params = captor.getValue().tableInput().parameters();
    assertEquals("true", params.get(GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM));
    assertEquals("v", params.get("k"));
    assertEquals(metadataLocation, params.get(METADATA_LOCATION));
    assertEquals("registered", captor.getValue().tableInput().description());
    verify(mockIcebergCatalog, never()).buildTable(any(TableIdentifier.class), any(Schema.class));
  }

  @Test
  void testRegisterIcebergTableRefusesDifferentColumnsAndDropsGlueEntry() {
    String metadataLocation = LOCATION + "/metadata/00001.metadata.json";
    Table registered = mockIcebergTable();
    when(mockIcebergCatalog.registerTable(TableIdentifier.of(DB, TABLE), metadataLocation))
        .thenReturn(registered);
    GlueColumn[] cols = {
      GlueColumn.builder().withName("bogus").withType(Types.DateType.get()).build()
    };

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.createTable(
                    NameIdentifier.of("cat", "ns", DB, TABLE),
                    cols,
                    null,
                    Map.of(
                        GlueConstants.TABLE_FORMAT, "iceberg", METADATA_LOCATION, metadataLocation),
                    new Transform[0],
                    Distributions.NONE,
                    null,
                    Indexes.EMPTY_INDEXES));
    assertTrue(e.getMessage().contains("bogus"), e.getMessage());
    verify(mockIcebergCatalog).dropTable(TableIdentifier.of(DB, TABLE), false);
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testRegisterIcebergTableRefusesLocation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                NameIdentifier.of("cat", "ns", DB, TABLE),
                icebergColumns(),
                null,
                Map.of(
                    GlueConstants.TABLE_FORMAT,
                    "iceberg",
                    METADATA_LOCATION,
                    LOCATION + "/metadata/00001.metadata.json",
                    GlueConstants.LOCATION,
                    LOCATION),
                new Transform[0],
                Distributions.NONE,
                null,
                Indexes.EMPTY_INDEXES));
    verify(mockIcebergCatalog, never()).registerTable(any(TableIdentifier.class), any());
  }

  @Test
  void testAlterRegisteredIcebergTableChangesOnlyGlueParameters() {
    stubGlueTable(registeredParams());
    stubIcebergTable();

    ops.alterTable(NameIdentifier.of("cat", "ns", DB, TABLE), TableChange.setProperty("k1", "v1"));

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockClient).updateTable(captor.capture());
    assertEquals("v1", captor.getValue().tableInput().parameters().get("k1"));
    assertEquals("3", captor.getValue().versionId());
    verify(mockIcebergCatalog, never()).renameTable(any(), any());
    verify(mockIcebergCatalog, Mockito.times(1)).loadTable(any(TableIdentifier.class));
  }

  @Test
  void testAlterRegisteredIcebergTableRefusesColumnAndReservedChanges() {
    stubGlueTable(registeredParams());
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                ident, TableChange.addColumn(new String[] {"c"}, Types.IntegerType.get())));
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTable(ident, TableChange.setProperty(METADATA_LOCATION, "s3://b/x.json")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                ident, TableChange.removeProperty(GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM)));
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testPurgeIcebergTableDeletesFiles() {
    stubGlueTable(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE));
    when(mockIcebergCatalog.dropTable(TableIdentifier.of(DB, TABLE), true)).thenReturn(true);

    assertTrue(ops.purgeTable(NameIdentifier.of("cat", "ns", DB, TABLE)));
    verify(mockIcebergCatalog).dropTable(TableIdentifier.of(DB, TABLE), true);
  }

  @Test
  void testPurgeRefusedForHiveAndRegisteredTables() {
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    stubGlueTable(Map.of("table_type", "HIVE"));
    assertThrows(UnsupportedOperationException.class, () -> ops.purgeTable(ident));

    stubGlueTable(registeredParams());
    assertThrows(UnsupportedOperationException.class, () -> ops.purgeTable(ident));
    verify(mockIcebergCatalog, never()).dropTable(any(TableIdentifier.class), Mockito.anyBoolean());
  }

  @Test
  void testIcebergPropertiesCannotShadowGlueFormat() {
    stubGlueTable(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE));
    Table icebergTable = mockIcebergTable();
    when(icebergTable.properties())
        .thenReturn(Map.of(TABLE_TYPE_PARAM, "HIVE", GlueConstants.TABLE_FORMAT, "HIVE", "k", "v"));
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class))).thenReturn(icebergTable);

    GlueTable table = ops.loadTable(NameIdentifier.of("cat", "ns", DB, TABLE));

    assertEquals(ICEBERG_TABLE_TYPE_VALUE, table.properties().get(TABLE_TYPE_PARAM));
    assertEquals("ICEBERG", table.properties().get(GlueConstants.TABLE_FORMAT));
    assertEquals("v", table.properties().get("k"));
    assertThrows(UnsupportedOperationException.class, table::supportPartitions);
  }

  @Test
  void testIcebergAlterRefusesFormatProperties() {
    stubGlueTable(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE));
    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    for (TableChange change :
        new TableChange[] {
          TableChange.setProperty(TABLE_TYPE_PARAM, "HIVE"),
          TableChange.setProperty(GlueConstants.TABLE_FORMAT, "HIVE"),
          TableChange.removeProperty(METADATA_LOCATION)
        }) {
      assertThrows(IllegalArgumentException.class, () -> ops.alterTable(ident, change));
    }
    verify(mockIcebergCatalog, never()).loadTable(any(TableIdentifier.class));
  }

  private static Map<String, String> registeredParams() {
    return Map.of(
        TABLE_TYPE_PARAM,
        ICEBERG_TABLE_TYPE_VALUE,
        METADATA_LOCATION,
        LOCATION + "/metadata/00001.metadata.json",
        GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM,
        "true");
  }

  private void stubGlueTable(Map<String, String> params) {
    software.amazon.awssdk.services.glue.model.Table rawTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .versionId("3")
            .parameters(params)
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build());
  }

  /** Makes every Iceberg table load return a one-column, unpartitioned table. */
  private Table stubIcebergTable() {
    Table table = mockIcebergTable();
    when(mockIcebergCatalog.loadTable(any(TableIdentifier.class))).thenReturn(table);
    return table;
  }

  private static Table mockIcebergTable() {
    Schema schema =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.LongType.get()));
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(schema);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.sortOrder()).thenReturn(SortOrder.unsorted());
    when(table.properties()).thenReturn(Map.of());
    return table;
  }

  @Test
  void testAlterIcebergTableRenameOntoExistingTableIsAlreadyExists() {
    software.amazon.awssdk.services.glue.model.Table rawTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(Map.of(TABLE_TYPE_PARAM, ICEBERG_TABLE_TYPE_VALUE))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build());
    Mockito.doThrow(
            new org.apache.iceberg.exceptions.AlreadyExistsException(
                "Table already exists: %s.%s.taken",
                GlueIcebergTableHelper.ICEBERG_CATALOG_NAME, DB))
        .when(mockIcebergCatalog)
        .renameTable(any(TableIdentifier.class), any(TableIdentifier.class));

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    TableAlreadyExistsException e =
        assertThrows(
            TableAlreadyExistsException.class,
            () -> ops.alterTable(ident, TableChange.rename("taken")));
    assertFalse(
        e.getMessage().contains(GlueIcebergTableHelper.ICEBERG_CATALOG_NAME), e.getMessage());
  }

  @Test
  void testCreateIcebergTableWithRejectedCredentialsIsForbidden() {
    Catalog.TableBuilder mockBuilder = mock(Catalog.TableBuilder.class, Mockito.RETURNS_SELF);
    when(mockIcebergCatalog.buildTable(any(TableIdentifier.class), any(Schema.class)))
        .thenReturn(mockBuilder);
    when(mockBuilder.create())
        .thenThrow(
            new RuntimeException(
                GlueException.builder()
                    .message("The security token included in the request is invalid")
                    .awsErrorDetails(
                        AwsErrorDetails.builder()
                            .errorCode("UnrecognizedClientException")
                            .errorMessage("The security token included in the request is invalid")
                            .build())
                    .build()));

    assertThrows(
        ForbiddenException.class,
        () ->
            ops.createTable(
                NameIdentifier.of("cat", "ns", DB, TABLE),
                icebergColumns(),
                null,
                Map.of(GlueConstants.TABLE_FORMAT, "iceberg", GlueConstants.LOCATION, LOCATION),
                new Transform[0],
                Distributions.NONE,
                null,
                Indexes.EMPTY_INDEXES));
  }

  @Test
  void testCreateIcebergTableRequiresS3Location() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.createTable(
                    NameIdentifier.of("cat", "ns", DB, TABLE),
                    icebergColumns(),
                    null,
                    Map.of(
                        GlueConstants.TABLE_FORMAT,
                        "iceberg",
                        GlueConstants.LOCATION,
                        "hdfs://nn:8020/warehouse/ice1"),
                    new Transform[0],
                    Distributions.NONE,
                    null,
                    Indexes.EMPTY_INDEXES));
    assertTrue(e.getMessage().contains("hdfs://nn:8020/warehouse/ice1"), e.getMessage());
    verify(mockIcebergCatalog, never()).buildTable(any(TableIdentifier.class), any(Schema.class));
  }

  private static GlueColumn[] icebergColumns() {
    return new GlueColumn[] {
      GlueColumn.builder().withName("id").withType(Types.LongType.get()).withNullable(false).build()
    };
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
    stubIcebergTable();

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
    stubIcebergTable();

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

  /**
   * Glue stores table_type as a free-form string and different writers use different casing, so the
   * register-mode check must not treat a lower-case "iceberg" as an externally registered table.
   */
  @Test
  void testAlterIcebergTableWithLowerCaseTableTypeRoutesToIcebergSdk() {
    String newName = "ice1_renamed";
    software.amazon.awssdk.services.glue.model.Table rawTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(TABLE)
            .parameters(Map.of(TABLE_TYPE_PARAM, "iceberg", "metadata_location", LOCATION))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    software.amazon.awssdk.services.glue.model.Table renamedTable =
        software.amazon.awssdk.services.glue.model.Table.builder()
            .name(newName)
            .parameters(Map.of(TABLE_TYPE_PARAM, "iceberg"))
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    when(mockClient.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(rawTable).build())
        .thenReturn(GetTableResponse.builder().table(renamedTable).build());
    stubIcebergTable();

    NameIdentifier ident = NameIdentifier.of("cat", "ns", DB, TABLE);
    GlueTable result = ops.alterTable(ident, TableChange.rename(newName));

    verify(mockIcebergCatalog)
        .renameTable(TableIdentifier.of(DB, TABLE), TableIdentifier.of(DB, newName));
    verify(mockClient, never()).updateTable(any(UpdateTableRequest.class));
    assertEquals(newName, result.name());
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
