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
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.type.TimestampType;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.glue.GlueMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataColumnCheck {

  private static final ColumnMetadata MICROS_COLUMN =
      ColumnMetadata.builder().setName("ts").setType(TimestampType.TIMESTAMP_MICROS).build();

  private final CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
  private final ConnectorSession session = mock(ConnectorSession.class);
  private final ConnectorTableHandle tableHandle =
      new GravitinoTableHandle("db", "tbl", mock(ConnectorTableHandle.class));
  private final GravitinoColumnHandle columnHandle =
      new GravitinoColumnHandle("ts", mock(ColumnHandle.class));
  private final GravitinoMetadata metadata =
      new StubGravitinoMetadata(
          catalogMetadata,
          new GlueMetadataAdapter(ImmutableList.of()),
          mock(ConnectorMetadata.class));

  @Test
  public void testAddedColumnIsCheckedAgainstTableFormat() {
    mockTableType("EXTERNAL_TABLE");
    assertThrows(TrinoException.class, () -> metadata.createColumn(tableHandle, MICROS_COLUMN));

    mockTableType("ICEBERG");
    metadata.createColumn(tableHandle, MICROS_COLUMN);
  }

  @Test
  public void testSetColumnTypeIsCheckedAgainstTableFormat() {
    mockTableType("EXTERNAL_TABLE");
    assertThrows(
        TrinoException.class,
        () ->
            metadata.setColumnType(
                session, tableHandle, columnHandle, TimestampType.TIMESTAMP_MICROS));
    verify(catalogMetadata, never()).setColumnType(any(), any(), any());

    mockTableType("ICEBERG");
    metadata.setColumnType(session, tableHandle, columnHandle, TimestampType.TIMESTAMP_MICROS);
    verify(catalogMetadata)
        .setColumnType(any(), eq("ts"), eq(Types.TimestampType.withoutTimeZone(6)));
  }

  private void mockTableType(String tableType) {
    Table table = mock(Table.class);
    when(table.name()).thenReturn("tbl");
    when(table.columns()).thenReturn(new Column[0]);
    when(table.properties()).thenReturn(Map.of("table_type", tableType));
    when(table.partitioning()).thenReturn(new Transform[0]);
    when(table.sortOrder()).thenReturn(new SortOrder[0]);
    when(table.distribution()).thenReturn(Distributions.NONE);
    when(table.index()).thenReturn(Indexes.EMPTY_INDEXES);
    GravitinoTable gravitinoTable = new GravitinoTable("db", "tbl", table);
    when(catalogMetadata.getTable("db", "tbl")).thenReturn(gravitinoTable);
  }

  private static final class StubGravitinoMetadata extends GravitinoMetadata {
    private StubGravitinoMetadata(
        CatalogConnectorMetadata catalogConnectorMetadata,
        GlueMetadataAdapter metadataAdapter,
        ConnectorMetadata internalMetadata) {
      super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    }
  }
}
