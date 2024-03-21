/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/** Database operations for Doris. */
public class DorisDatabaseOperations extends JdbcDatabaseOperations {
  // TODO: add implementation for doris catalog

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String generateDropDatabaseSql(String databaseName, boolean cascade) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ResultSet getSchema(Connection connection, String schemaName) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
