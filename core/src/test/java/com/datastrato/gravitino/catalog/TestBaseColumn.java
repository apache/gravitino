/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.datastrato.gravitino.connector.BaseColumn;
import com.datastrato.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

final class BaseColumnExtension extends BaseColumn {
  private BaseColumnExtension() {}

  public static class Builder extends BaseColumnBuilder<Builder, BaseColumnExtension> {

    @Override
    protected BaseColumnExtension internalBuild() {
      BaseColumnExtension column = new BaseColumnExtension();
      column.name = name;
      column.comment = comment;
      column.dataType = dataType;
      column.nullable = nullable;
      return column;
    }
  }
}

public class TestBaseColumn {

  @Test
  void testColumnFields() {
    BaseColumn column =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.IntegerType.get())
            .build();

    assertEquals("testColumnName", column.name());
    assertEquals("testColumnComment", column.comment());
    assertEquals(Types.IntegerType.get(), column.dataType());
  }

  @Test
  void testEqualsAndHashCode() {
    BaseColumn column1 =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column2 =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column3 =
        new BaseColumnExtension.Builder()
            .withName("differentColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column4 =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.IntegerType.get())
            .build();

    assertEquals(column1, column2);
    assertEquals(column1.hashCode(), column2.hashCode());
    assertNotEquals(column1, column3);
    assertNotEquals(column1, column4);
  }
}
