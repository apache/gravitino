/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
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
    ColumnDTO[] columns =
        new ColumnDTO[] {
          new ColumnDTO.Builder()
              .withName("f1")
              .withDataType(Types.StringType.get())
              .withComment("f1 column")
              .build(),
          new ColumnDTO.Builder().withName("f2").withDataType(Types.IntegerType.get()).build()
        };
    Map<String, String> properties = new HashMap<>();
    properties.put("format", "TEXTFILE");
    TableDTO tableDTO =
        new TableDTO.Builder()
            .withName("table1")
            .withColumns(columns)
            .withComment("test table")
            .withProperties(properties)
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    GravitinoTable table = new GravitinoTable("db1", "table1", tableDTO);

    assertEquals(table.getName(), tableDTO.name());
    assertEquals(table.getSchemaName(), "db1");
    assertEquals(table.getColumns().size(), tableDTO.columns().length);
    for (int i = 0; i < table.getColumns().size(); i++) {
      assertEquals(table.getColumns().get(i).getName(), tableDTO.columns()[i].name());
    }
    assertEquals(table.getComment(), tableDTO.comment());
    assertEquals(table.getProperties(), tableDTO.properties());

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
    assertEquals(tableMetadata.getComment().get(), tableDTO.comment());
  }

  @Test
  public void testGravitinoTableWithOutComment() {
    ColumnDTO[] columns =
        new ColumnDTO[] {
          new ColumnDTO.Builder()
              .withName("f1")
              .withDataType(Types.StringType.get())
              .withComment("f1 column")
              .build(),
          new ColumnDTO.Builder().withName("f2").withDataType(Types.IntegerType.get()).build()
        };
    Map<String, String> properties = new HashMap<>();
    properties.put("format", "TEXTFILE");
    TableDTO tableDTO =
        new TableDTO.Builder()
            .withName("table1")
            .withColumns(columns)
            .withProperties(properties)
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    GravitinoTable table = new GravitinoTable("db1", "table1", tableDTO);
    assertEquals(table.getComment(), null);

    CatalogConnectorMetadataAdapter adapter =
        new HiveMetadataAdapter(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    ConnectorTableMetadata tableMetadata = adapter.getTableMetadata(table);
    assertTrue(tableMetadata.getComment().isEmpty());
  }
}
