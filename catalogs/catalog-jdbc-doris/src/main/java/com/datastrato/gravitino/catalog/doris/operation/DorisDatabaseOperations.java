/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.doris.utils.DorisUtils;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.meta.AuditInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
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
      return sqlBuilder.toString();
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      String query = "SHOW TABLES IN " + databaseName;
      try (Statement statement = connection.createStatement()) {
        // Execute the query and check if there exists any tables in the database
        try (ResultSet resultSet = statement.executeQuery(query)) {
          if (resultSet.next()) {
            throw new IllegalStateException(
                String.format(
                    "Database %s is not empty, the value of cascade should be true.",
                    databaseName));
          }
        }
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
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
          Map<String, String> properties = getDatabaseProperties(connection, databaseName);
          return new JdbcSchema.Builder()
              .withName(schemaName)
              .withProperties(properties)
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
        }
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected Map<String, String> getDatabaseProperties(Connection connection, String databaseName)
      throws SQLException {

    String showCreateDatabaseSql = String.format("SHOW CREATE DATABASE `%s`", databaseName);

    StringBuilder createDatabaseSb = new StringBuilder();
    try (PreparedStatement statement = connection.prepareStatement(showCreateDatabaseSql)) {
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          createDatabaseSb.append(resultSet.getString("Create Database"));
        }
      }
    }

    String createDatabaseSql = createDatabaseSb.toString();

    if (StringUtils.isEmpty(createDatabaseSql)) {
      throw new NoSuchTableException(
          "Database %s does not exist in %s.", databaseName, connection.getCatalog());
    }

    return Collections.unmodifiableMap(DorisUtils.extractPropertiesFromSql(createDatabaseSql));
  }
}
