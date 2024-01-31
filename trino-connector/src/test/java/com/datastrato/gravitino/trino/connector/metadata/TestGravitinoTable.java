/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.hive.HiveMetadataAdapter;
import io.trino.spi.connector.ConnectorTableMetadata;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class TestGravitinoTable {

  @Test
  public void testGravitinoTable() {
    Column[] columns = {
      Column.of("f1", Types.StringType.get(), "f1 column"), Column.of("f2", Types.IntegerType.get())
    };
    Map<String, String> properties = new HashMap<>();
    properties.put("format", "TEXTFILE");
    Table mockTable = mockTable("table1", columns, "test table", properties);

    GravitinoTable table = new GravitinoTable("db1", "table1", mockTable);

    assertEquals(table.getName(), mockTable.name());
    assertEquals(table.getSchemaName(), "db1");
    assertEquals(table.getColumns().size(), mockTable.columns().length);
    for (int i = 0; i < table.getColumns().size(); i++) {
      assertEquals(table.getColumns().get(i).getName(), mockTable.columns()[i].name());
    }
    assertEquals(table.getComment(), mockTable.comment());
    assertEquals(table.getProperties(), mockTable.properties());

    CatalogConnectorMetadataAdapter adapter =
        new HiveMetadataAdapter(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    ConnectorTableMetadata tableMetadata = adapter.getTableMetadata(table);
    assertEquals(tableMetadata.getColumns().size(), table.getColumns().size());
    assertEquals(tableMetadata.getTableSchema().getTable().getSchemaName(), "db1");
    assertEquals(tableMetadata.getTableSchema().getTable().getTableName(), table.getName());

    for (int i = 0; i < table.getColumns().size(); i++) {
      assertEquals(
          tableMetadata.getColumns().get(i).getName(), table.getColumns().get(i).getName());
    }
    assertTrue(tableMetadata.getComment().isPresent());
    assertEquals(tableMetadata.getComment().get(), mockTable.comment());
  }

  @Test
  public void testGravitinoTableWithOutComment() {
    Column[] columns = {
      Column.of("f1", Types.StringType.get(), "f1 column"), Column.of("f2", Types.IntegerType.get())
    };
    Map<String, String> properties = new HashMap<>();
    properties.put("format", "TEXTFILE");

    Table mockTable = mockTable("table1", columns, null, properties);

    GravitinoTable table = new GravitinoTable("db1", "table1", mockTable);
    assertNull(table.getComment());

    CatalogConnectorMetadataAdapter adapter =
        new HiveMetadataAdapter(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    ConnectorTableMetadata tableMetadata = adapter.getTableMetadata(table);
    assertTrue(tableMetadata.getComment().isEmpty());
  }

  public static Table mockTable(
      String tableName, Column[] columns, String comment, Map<String, String> properties) {
    Table table = mock(Table.class);
    when(table.name()).thenReturn(tableName);
    when(table.columns()).thenReturn(columns);
    when(table.comment()).thenReturn(comment);
    when(table.properties()).thenReturn(properties);
    when(table.partitioning()).thenReturn(new Transform[0]);
    when(table.sortOrder()).thenReturn(new SortOrder[0]);
    when(table.distribution()).thenReturn(Distributions.NONE);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(table.auditInfo()).thenReturn(mockAudit);

    return table;
  }

  public static Table mockTable(
      String tableName,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      SortOrder[] sortOrder,
      Distribution distribution) {
    Table table = mock(Table.class);
    when(table.name()).thenReturn(tableName);
    when(table.columns()).thenReturn(columns);
    when(table.comment()).thenReturn(comment);
    when(table.properties()).thenReturn(properties);
    when(table.partitioning()).thenReturn(partitioning);
    when(table.sortOrder()).thenReturn(sortOrder);
    when(table.distribution()).thenReturn(distribution);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(table.auditInfo()).thenReturn(mockAudit);

    return table;
  }
}
