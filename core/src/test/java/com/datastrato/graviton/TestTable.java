/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.meta.rel.BaseTable;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SortOrder;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestTable extends BaseTable {

  protected Distribution distribution;

  protected SortOrder[] sortOrders;

  public static class Builder extends BaseTable.BaseTableBuilder<Builder, TestTable> {
    private Distribution distribution;

    private SortOrder[] sortOrders;

    public Builder withDistribution(Distribution distribution) {
      this.distribution = distribution;
      return this;
    }

    public Builder withSortOrders(SortOrder[] sortOrders) {
      this.sortOrders = sortOrders;
      return this;
    }

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
      table.distribution = distribution;
      table.sortOrders = sortOrders;
      table.partitions = partitions;
      return table;
    }
  }
}
