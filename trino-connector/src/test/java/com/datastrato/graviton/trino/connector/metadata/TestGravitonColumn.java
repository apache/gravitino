/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.dto.rel.ColumnDTO;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitonColumn {

  @Test
  void testGravitonColumn() {
    ColumnDTO columnDTO =
        new ColumnDTO.Builder()
            .withName("f1")
            .withComment("test column")
            .withDataType(TypeCreator.NULLABLE.STRING)
            .build();

    GravitonColumn column = new GravitonColumn(columnDTO, 0);

    Assertions.assertEquals(column.getName(), columnDTO.name());
    Assertions.assertEquals(column.getIndex(), 0);
    Assertions.assertEquals(column.getComment(), columnDTO.comment());
    Assertions.assertEquals(column.getType(), columnDTO.dataType());
  }
}
