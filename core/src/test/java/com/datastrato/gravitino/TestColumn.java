/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.BaseColumn;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class TestColumn extends BaseColumn {

  private TestColumn() {}

  public static class Builder extends BaseColumn.BaseColumnBuilder<Builder, TestColumn> {

    @Override
    protected TestColumn internalBuild() {
      TestColumn column = new TestColumn();

      column.name = name;
      column.comment = comment;
      column.dataType = dataType;
      column.nullable = nullable;
      column.defaultValue = defaultValue;

      return column;
    }
  }
}
