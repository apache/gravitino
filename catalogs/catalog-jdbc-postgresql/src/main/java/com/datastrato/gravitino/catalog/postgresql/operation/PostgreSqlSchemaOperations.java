/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.meta.AuditInfo;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/** Database operations for PostgreSQL. */
public class PostgreSqlSchemaOperations extends JdbcDatabaseOperations {

  public static final Set<String> SYS_PG_DATABASE_NAMES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("pg_toast");
              add("pg_catalog");
              add("information_schema");
            }
          });

  private String database;

  @Override
  public void initialize(
      DataSource dataSource, JdbcExceptionConverter exceptionMapper, Map<String, String> conf) {
    super.initialize(dataSource, exceptionMapper, conf);
    database = new JdbcConfig(conf).getJdbcDatabase();
  }

  @Override
  public JdbcSchema load(String schema) throws NoSuchSchemaException {
    try (Connection connection = getConnection()) {
      ResultSet resultSet = getSchema(connection, schema);

      boolean found = false;
      while (resultSet.next()) {
        if (Objects.equals(resultSet.getString(1), schema)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new NoSuchSchemaException("No such schema: %s", schema);
      }

      String comment = getSchemaComment(schema, connection);
      return JdbcSchema.builder()
          .withName(schema)
          .withComment(comment)
          .withAuditInfo(AuditInfo.EMPTY)
          .withProperties(Collections.emptyMap())
          .build();
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  @Override
  public List<String> listDatabases() {
    List<String> result = new ArrayList<>();
    try (Connection connection = getConnection()) {
      ResultSet resultSet = getSchema(connection, null);
      while (resultSet.next()) {
        String schemaName = resultSet.getString(1);
        if (!isSystemDatabase(schemaName)) {
          result.add(resultSet.getString(1));
        }
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
    return result;
  }

  @Override
  public String generateCreateDatabaseSql(
      String schema, String comment, Map<String, String> properties) {
    if (MapUtils.isNotEmpty(properties)) {
      throw new UnsupportedOperationException(
          "PostgreSQL does not support properties on database create.");
    }

    StringBuilder sqlBuilder = new StringBuilder("CREATE SCHEMA " + schema + ";");
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append("COMMENT ON SCHEMA ")
          .append(schema)
          .append(" IS '")
          .append(comment)
          .append("'");
    }
    return sqlBuilder.toString();
  }

  /**
   * Get the schema with the given name.
   *
   * <p>Note: This method will return a result set that may contain multiple rows as the schemaName
   * in `getSchemas` is a pattern. The result set will contain all schemas that match the pattern.
   *
   * <p>Database in PG corresponds to Catalog in JDBC. Schema in PG corresponds to Schema in JDBC.
   *
   * @param connection the connection to the database
   * @param schemaName the name of the schema
   */
  private ResultSet getSchema(Connection connection, String schemaName) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getSchemas(database, schemaName);
  }

  @Override
  public String generateDropDatabaseSql(String schema, boolean cascade) {
    StringBuilder sqlBuilder = new StringBuilder(String.format("DROP SCHEMA %s", schema));
    if (cascade) {
      sqlBuilder.append(" CASCADE");
    }
    return sqlBuilder.toString();
  }

  @Override
  protected Connection getConnection() throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    return connection;
  }

  @Override
  protected boolean isSystemDatabase(String dbName) {
    return SYS_PG_DATABASE_NAMES.contains(dbName.toLowerCase(Locale.ROOT));
  }

  private String getShowSchemaCommentSql(String schema) {
    return String.format("SELECT obj_description('%s'::regnamespace) as comment", schema);
  }

  private String getSchemaComment(String schema, Connection connection) throws SQLException {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(getShowSchemaCommentSql(schema))) {
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          return resultSet.getString("comment");
        }
      }
    }
    return null;
  }
}
