/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.shaded.io.substrait.type.TypeCreator;
import org.testng.annotations.Test;

public class TestGravitinoColumn {

  @Test
  public void testGravitinoColumn() {
    ColumnDTO columnDTO =
        new ColumnDTO.Builder()
            .withName("f1")
            .withComment("test column")
            .withDataType(TypeCreator.NULLABLE.STRING)
            .build();

    GravitinoColumn column = new GravitinoColumn(columnDTO, 0);

    assertEquals(column.getName(), columnDTO.name());
    assertEquals(column.getIndex(), 0);
    assertEquals(column.getComment(), columnDTO.comment());
    assertEquals(column.getType(), columnDTO.dataType());
  }
}
