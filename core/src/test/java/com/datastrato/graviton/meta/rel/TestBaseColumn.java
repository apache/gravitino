/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.google.common.collect.Maps;
import io.substrait.type.Type;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestBaseColumn {

  private final class BaseColumnExtension extends BaseColumn {
    @Override
    public void validate() {}
  }

  @Test
  void testColumnFields() {
    BaseColumn column = new BaseColumnExtension();

    column.name = "testColumnName";
    column.comment = "testColumnComment";
    column.dataType = Type.withNullability(false).STRING;

    Map<Field, Object> expectedFields = Maps.newHashMap();
    expectedFields.put(BaseColumn.NAME, "testColumnName");
    expectedFields.put(BaseColumn.COMMENT, "testColumnComment");
    expectedFields.put(BaseColumn.TYPE, Type.withNullability(false).STRING);

    assertEquals("testColumnName", column.name());
    assertEquals("testColumnComment", column.comment());
    assertEquals(Type.withNullability(false).STRING, column.dataType());
    assertEquals(Entity.EntityType.COLUMN, column.type());
    assertEquals(expectedFields, column.fields());
  }

  @Test
  void testColumnType() {
    BaseColumn table = new BaseColumnExtension();

    assertEquals(Entity.EntityType.COLUMN, table.type());
  }

  @Test
  void testEqualsAndHashCode() {
    BaseColumn column1 = new BaseColumnExtension();
    column1.name = "testColumnName";
    column1.comment = "testColumnComment";
    column1.dataType = Type.withNullability(false).STRING;

    BaseColumn column2 = new BaseColumnExtension();
    column2.name = "testColumnName";
    column2.comment = "testColumnComment";
    column2.dataType = Type.withNullability(false).STRING;

    BaseColumn column3 = new BaseColumnExtension();
    column3.name = "differntColumnName";
    column3.comment = "testColumnComment";
    column3.dataType = Type.withNullability(false).STRING;

    BaseColumn column4 = new BaseColumnExtension();
    column4.name = "testColumnName";
    column4.comment = "testColumnComment";
    column4.dataType = Type.withNullability(false).I32;

    assertEquals(column1, column2);
    assertEquals(column1.hashCode(), column2.hashCode());
    assertNotEquals(column1, column3);
    assertNotEquals(column1, column4);
  }
}
