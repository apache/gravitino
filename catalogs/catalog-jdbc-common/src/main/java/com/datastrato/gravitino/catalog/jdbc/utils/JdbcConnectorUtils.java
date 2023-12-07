/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcConnectorUtils {

  public static final String[] TABLE_TYPES = {"TABLE"};

  private JdbcConnectorUtils() {}

  /**
   * Execute a SQL update statement against the given datasource.
   *
   * @param connection The connection to attempt to execute an update against
   * @param sql The sql to execute
   * @return The number of rows updated or exception
   * @throws SQLException on error during execution of the update to the underlying SQL data store
   */
  public static int executeUpdate(final Connection connection, final String sql)
      throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      return statement.executeUpdate(sql);
    }
  }
}
