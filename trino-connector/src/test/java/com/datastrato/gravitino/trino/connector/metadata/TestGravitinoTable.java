/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.rel.types.Types;
import java.time.Instant;
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
          new ColumnDTO.Builder()
              .withName("f2")
              .withDataType(Types.IntegerType.get())
              .withComment("f2 column")
              .build()
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
  }
}
