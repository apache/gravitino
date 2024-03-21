/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Types;
import org.testng.annotations.Test;

public class TestGravitinoColumn {

  @Test
  public void testGravitinoColumn() {
    Column column = Column.of("f1", Types.StringType.get(), "test column");
    GravitinoColumn gravitinoColumn = new GravitinoColumn(column, 0);

    assertEquals(gravitinoColumn.getName(), column.name());
    assertEquals(gravitinoColumn.getIndex(), 0);
    assertEquals(gravitinoColumn.getComment(), column.comment());
    assertEquals(gravitinoColumn.getType(), column.dataType());
  }
}
