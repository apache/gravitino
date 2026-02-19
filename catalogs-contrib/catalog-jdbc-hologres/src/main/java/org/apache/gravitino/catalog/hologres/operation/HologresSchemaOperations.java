/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.hologres.operation;

import static org.apache.gravitino.catalog.hologres.operation.HologresTableOperations.HOLO_QUOTE;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;

/**
 * Schema (Database) operations for Hologres.
 *
 * <p>Hologres uses the PostgreSQL schema concept where Database in PostgreSQL corresponds to
 * Catalog in JDBC, and Schema in PostgreSQL corresponds to Schema in JDBC.
 */
public class HologresSchemaOperations extends JdbcDatabaseOperations {

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
      if (!schemaExists(connection, schema)) {
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
    try (Connection connection = getConnection();
        ResultSet resultSet = getSchema(connection, null)) {
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
          "Hologres does not support properties on schema create.");
    }

    StringBuilder sqlBuilder = new StringBuilder("CREATE SCHEMA \"" + schema + "\";");
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append("COMMENT ON SCHEMA ")
          .append(HOLO_QUOTE)
          .append(schema)
          .append(HOLO_QUOTE)
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
   * <p>Database in Hologres (PostgreSQL) corresponds to Catalog in JDBC. Schema in Hologres
   * corresponds to Schema in JDBC.
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
    StringBuilder sqlBuilder =
        new StringBuilder(String.format("DROP SCHEMA %s%s%s", HOLO_QUOTE, schema, HOLO_QUOTE));
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
  protected String generateDatabaseExistSql(String databaseName) {
    return String.format(
        "SELECT n.datname FROM pg_catalog.pg_database n where n.datname='%s'", databaseName);
  }

  @Override
  protected boolean supportSchemaComment() {
    return true;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of(
        // PostgreSQL system schemas
        "pg_toast",
        "pg_catalog",
        "information_schema",
        // Hologres internal schemas
        "hologres",
        "hg_internal",
        "hg_recyclebin",
        "hologres_object_table",
        "hologres_sample",
        "hologres_streaming_mv",
        "hologres_statistic");
  }

  /**
   * Check if the schema exists in the database.
   *
   * @param connection the connection to the database
   * @param schema the name of the schema
   * @return true if the schema exists, false otherwise
   */
  public boolean schemaExists(Connection connection, String schema) throws SQLException {
    try (ResultSet resultSet = getSchema(connection, schema)) {
      while (resultSet.next()) {
        if (Objects.equals(resultSet.getString(1), schema)) {
          return true;
        }
      }
    }
    return false;
  }

  private String getShowSchemaCommentSql(String schema) {
    return String.format(
        "SELECT obj_description(n.oid, 'pg_namespace') AS comment\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname = '%s';\n",
        schema);
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
