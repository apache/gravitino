/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.catalog.jdbc.operation.TableOperation;
import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.ToString;

/** Represents a Jdbc Table entity in the jdbc table. */
@ToString
public class JdbcTable extends BaseTable {
  private String databaseName;
  private TableOperation tableOperation;

  private JdbcTable() {}

  @Override
  protected TableOperations newOps() {
    if (tableOperation == null) {
      throw new UnsupportedOperationException("Table partition operation is not supported yet");
    }
    return tableOperation.createJdbcTablePartitionOperations(databaseName, name);
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return (SupportsPartitions) ops();
  }

  /** A builder class for constructing JdbcTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, JdbcTable> {
    private String databaseName;
    private TableOperation tableOperation;

    /**
     * Sets the name of database.
     *
     * @param databaseName The name of database.
     * @return This Builder instance.
     */
    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    /**
     * Sets the table operation to be used for partition operations.
     *
     * @param tableOperation The instance of TableOperation.
     * @return This Builder instance.
     */
    public Builder withTableOperation(TableOperation tableOperation) {
      this.tableOperation = tableOperation;
      return this;
    }

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

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
      jdbcTable.proxyPlugin = proxyPlugin;
      jdbcTable.databaseName = databaseName;
      jdbcTable.tableOperation = tableOperation;
      return jdbcTable;
    }

    public String comment() {
      return comment;
    }

    public Map<String, String> properties() {
      return properties;
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
