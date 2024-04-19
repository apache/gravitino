/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestTable extends BaseTable {

  @Override
  protected TableOperations newOps() {
    throw new UnsupportedOperationException("TestTable does not support TableOperations.");
  }

  public static class Builder extends BaseTable.BaseTableBuilder<Builder, TestTable> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

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
      table.indexes = indexes;
      return table;
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
