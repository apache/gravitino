/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.substrait.type.TypeCreator;
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
            .withType(TypeCreator.NULLABLE.STRING)
            .build();

    assertEquals("testColumnName", column.name());
    assertEquals("testColumnComment", column.comment());
    assertEquals(TypeCreator.NULLABLE.STRING, column.dataType());
  }

  @Test
  void testEqualsAndHashCode() {
    BaseColumn column1 =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(TypeCreator.NULLABLE.STRING)
            .build();

    BaseColumn column2 =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(TypeCreator.NULLABLE.STRING)
            .build();

    BaseColumn column3 =
        new BaseColumnExtension.Builder()
            .withName("differentColumnName")
            .withComment("testColumnComment")
            .withType(TypeCreator.NULLABLE.STRING)
            .build();

    BaseColumn column4 =
        new BaseColumnExtension.Builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(TypeCreator.NULLABLE.I32)
            .build();

    assertEquals(column1, column2);
    assertEquals(column1.hashCode(), column2.hashCode());
    assertNotEquals(column1, column3);
    assertNotEquals(column1, column4);
  }
}
