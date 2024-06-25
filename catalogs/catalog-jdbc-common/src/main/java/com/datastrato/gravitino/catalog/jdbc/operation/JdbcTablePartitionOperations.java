/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.rel.SupportsPartitions;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

public abstract class JdbcTablePartitionOperations implements TableOperations, SupportsPartitions {
  protected final DataSource dataSource;
  protected final String databaseName;
  protected final String tableName;

  protected JdbcTablePartitionOperations(
      DataSource dataSource, String databaseName, String tableName) {
    checkArgument(dataSource != null, "dataSource is null");
    checkArgument(databaseName != null, "databaseName is null");
    checkArgument(tableName != null, "table is null");
    this.dataSource = dataSource;
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  protected Connection getConnection(String databaseName) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(databaseName);
    return connection;
  }

  @Override
  public void close() throws IOException {
    // Nothing to be closed.
  }
}
