/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.catalog.rel.BaseTable;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestTable extends BaseTable {

  public static class Builder extends BaseTable.BaseTableBuilder<Builder, TestTable> {

    @Override
    protected TestTable internalBuild() {
      TestTable table = new TestTable();
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.columns = columns;
      table.auditInfo = auditInfo;
      table.distribution = distribution;
      table.sortOrders = sortOrders;
      table.partitioning = partitioning;
      return table;
    }
  }
}
