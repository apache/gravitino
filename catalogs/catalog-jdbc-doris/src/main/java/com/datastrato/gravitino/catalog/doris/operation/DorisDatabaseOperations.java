/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.doris.utils.DorisUtils;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.meta.AuditInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Database operations for Doris. */
public class DorisDatabaseOperations extends JdbcDatabaseOperations {
  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    if (StringUtils.isNotEmpty(comment)) {
      LOG.warn(
          "Ignoring comment option on database create. doris does not support comment option on database create.");
    }
    StringBuilder sqlBuilder = new StringBuilder("CREATE DATABASE ");

    // Append database name
    sqlBuilder.append("`").append(databaseName).append("`");

    // Append properties
    sqlBuilder.append(DorisUtils.generatePropertiesSql(properties));

    String result = sqlBuilder.toString();
    LOG.info("Generated create database:{} sql: {}", databaseName, result);
    return result;
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    StringBuilder sqlBuilder = new StringBuilder("DROP DATABASE");
    sqlBuilder.append("`").append(databaseName).append("`");
    if (cascade) {
      sqlBuilder.append(" FORCE");
    }

    return sqlBuilder.toString();
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    try (final Connection connection = this.dataSource.getConnection()) {
      String query = "SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?";
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        preparedStatement.setString(1, databaseName);

        // Execute the query
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          if (!resultSet.next()) {
            throw new NoSuchSchemaException(
                "Database %s could not be found in information_schema.SCHEMATA", databaseName);
          }
          String schemaName = resultSet.getString("SCHEMA_NAME");
          return new JdbcSchema.Builder()
              .withName(schemaName)
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
        }
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }
}
