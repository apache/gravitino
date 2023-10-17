/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoColumn {

  @Test
  void testGravitinoColumn() {
    ColumnDTO columnDTO =
        new ColumnDTO.Builder()
            .withName("f1")
            .withComment("test column")
            .withDataType(TypeCreator.NULLABLE.STRING)
            .build();

    GravitinoColumn column = new GravitinoColumn(columnDTO, 0);

    Assertions.assertEquals(column.getName(), columnDTO.name());
    Assertions.assertEquals(column.getIndex(), 0);
    Assertions.assertEquals(column.getComment(), columnDTO.comment());
    Assertions.assertEquals(column.getType(), columnDTO.dataType());
  }
}
