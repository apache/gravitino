/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.catalog.rel.BaseTable;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.ToString;

/** Represents a Jdbc Table entity in the jdbc table. */
@ToString
@Getter
public class JdbcTable extends BaseTable {

  private JdbcTable() {}

  /** A builder class for constructing JdbcTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, JdbcTable> {

    /**
     * Internal method to build a JdbcTable instance using the provided values.
     *
     * @return A new JdbcTable instance with the configured values.
     */
    @Override
    protected JdbcTable internalBuild() {
      JdbcTable jdbcTable = new JdbcTable();
      jdbcTable.name = name;
      jdbcTable.comment = comment;
      jdbcTable.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      jdbcTable.auditInfo = auditInfo;
      jdbcTable.columns = columns;
      jdbcTable.partitioning = partitioning;
      jdbcTable.sortOrders = sortOrders;
      jdbcTable.indexes = indexes;
      return jdbcTable;
    }
  }
}
