/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoTable {

  @Test
  void testGravitinoTable() {
    ColumnDTO[] columns =
        new ColumnDTO[] {
          new ColumnDTO.Builder()
              .withName("f1")
              .withDataType(TypeCreator.NULLABLE.STRING)
              .withComment("f1 column")
              .build(),
          new ColumnDTO.Builder()
              .withName("f2")
              .withDataType(TypeCreator.NULLABLE.I32)
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

    Assertions.assertEquals(table.getName(), tableDTO.name());
    Assertions.assertEquals(table.getSchemaName(), "db1");
    Assertions.assertEquals(table.getColumns().size(), tableDTO.columns().length);
    for (int i = 0; i < table.getColumns().size(); i++) {
      Assertions.assertEquals(table.getColumns().get(i).getName(), tableDTO.columns()[i].name());
    }
    Assertions.assertEquals(table.getComment(), tableDTO.comment());
    Assertions.assertEquals(table.getProperties(), tableDTO.properties());
  }
}
