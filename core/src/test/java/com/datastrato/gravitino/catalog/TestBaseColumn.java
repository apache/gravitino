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

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

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

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}

public class TestBaseColumn {

  @Test
  void testColumnFields() {
    BaseColumn column =
        BaseColumnExtension.builder()
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
        BaseColumnExtension.builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column2 =
        BaseColumnExtension.builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column3 =
        BaseColumnExtension.builder()
            .withName("differentColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column4 =
        BaseColumnExtension.builder()
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
