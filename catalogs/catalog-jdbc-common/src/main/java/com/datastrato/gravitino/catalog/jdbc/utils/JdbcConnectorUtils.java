/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.utils;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcConnectorUtils {
  public static final ImmutableList<String> TABLE_TYPES = ImmutableList.of("TABLE");

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

  public static String[] getTableTypes() {
    return TABLE_TYPES.toArray(new String[0]);
  }
}
