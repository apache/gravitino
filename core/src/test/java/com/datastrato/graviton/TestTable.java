/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.meta.rel.BaseTable;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestTable extends BaseTable {

  private TestTable() {}

  public static class Builder extends BaseTable.BaseTableBuilder<Builder, TestTable> {

    @Override
    protected TestTable internalBuild() {
      TestTable table = new TestTable();

      table.id = id;
      table.name = name;
      table.comment = comment;
      table.namespace = namespace;
      table.properties = properties;
      table.columns = columns;
      table.auditInfo = auditInfo;

      return table;
    }
  }
}
