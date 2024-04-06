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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Database operations for Doris. */
public class DorisDatabaseOperations extends JdbcDatabaseOperations {
  public static final String COMMENT_KEY = "comment";

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    StringBuilder sqlBuilder = new StringBuilder();

    // Append database name
    sqlBuilder.append(String.format("CREATE DATABASE `%s`", databaseName));

    // Doris does not support setting schema comment, put comment in properties
    Map<String, String> newProperties = new HashMap<>(properties);
    newProperties.put(COMMENT_KEY, comment);

    // Append properties
    sqlBuilder.append(DorisUtils.generatePropertiesSql(newProperties));

    String result = sqlBuilder.toString();
    LOG.info("Generated create database:{} sql: {}", databaseName, result);
    return result;
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(String.format("DROP DATABASE `%s`", databaseName));
    if (cascade) {
      sqlBuilder.append(" FORCE");
      return sqlBuilder.toString();
    }

    // Execute the query and check if there exists any tables in the database
    String query = String.format("SHOW TABLES IN `%s`", databaseName);

    try (final Connection connection = this.dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      if (resultSet.next()) {
        throw new IllegalStateException(
            String.format(
                "Database %s is not empty, the value of cascade should be true.", databaseName));
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
    }

    return sqlBuilder.toString();
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    List<String> allDatabases = listDatabases();
    String dbName =
        allDatabases.stream()
            .filter(db -> db.equals(databaseName))
            .findFirst()
            .orElseThrow(
                () -> new NoSuchSchemaException("Database %s could not be found", databaseName));

    Map<String, String> properties = getDatabaseProperties(databaseName);

    // extract comment from properties
    String comment = properties.remove(COMMENT_KEY);

    return JdbcSchema.builder()
        .withName(dbName)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  protected Map<String, String> getDatabaseProperties(String databaseName) {

    String showCreateDatabaseSql = String.format("SHOW CREATE DATABASE `%s`", databaseName);
    StringBuilder createDatabaseSb = new StringBuilder();
    try (final Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(showCreateDatabaseSql);
        ResultSet resultSet = statement.executeQuery()) {
      while (resultSet.next()) {
        createDatabaseSb.append(resultSet.getString("Create Database"));
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
    }

    String createDatabaseSql = createDatabaseSb.toString();

    if (StringUtils.isEmpty(createDatabaseSql)) {
      throw new NoSuchTableException("Database %s does not exist.", databaseName);
    }

    return DorisUtils.extractPropertiesFromSql(createDatabaseSql);
  }
}
