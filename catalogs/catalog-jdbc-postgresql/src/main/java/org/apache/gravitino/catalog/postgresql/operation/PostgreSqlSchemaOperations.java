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
package org.apache.gravitino.catalog.postgresql.operation;

import static org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils.escapeSqlLiteral;
import static org.apache.gravitino.catalog.postgresql.operation.PostgreSqlTableOperations.PG_QUOTE;

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
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.meta.AuditInfo;

/** Database operations for PostgreSQL. */
public class PostgreSqlSchemaOperations extends JdbcDatabaseOperations {

  private static final String EXTERNAL_DEPENDENTS_SQL =
      "WITH RECURSIVE dependents(classid, objid, objsubid) AS ( "
          + "  SELECT 'pg_catalog.pg_namespace'::regclass::oid, oid, 0 "
          + "  FROM pg_catalog.pg_namespace WHERE nspname = ? "
          + "  UNION "
          + "  SELECT next_object.classid, next_object.objid, next_object.objsubid "
          + "  FROM dependents current_object "
          + "  CROSS JOIN LATERAL ( "
          + "    SELECT d.classid, d.objid, d.objsubid FROM pg_catalog.pg_depend d "
          + "    WHERE d.refclassid = current_object.classid AND d.refobjid = current_object.objid "
          + "      AND (current_object.objsubid = 0 OR d.refobjsubid = current_object.objsubid) "
          + "    UNION "
          + "    SELECT d.refclassid, d.refobjid, d.refobjsubid FROM pg_catalog.pg_depend d "
          + "    WHERE d.classid = current_object.classid AND d.objid = current_object.objid "
          + "      AND d.objsubid = current_object.objsubid AND d.deptype = 'i' "
          + "  ) next_object "
          + ") "
          + "SELECT DISTINCT pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent "
          + "FROM dependents d "
          + "CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid, d.objid, d.objsubid) info "
          // TOAST tables and their indexes/types are PostgreSQL-managed storage for the tables
          // being dropped, not user objects in another schema. Keep traversing them, but do not
          // report the internal pg_toast namespace as an external dependent.
          + "WHERE info.schema IS NOT NULL AND info.schema <> ? AND info.schema <> 'pg_toast'; ";

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
          "PostgreSQL does not support properties on database create.");
    }

    StringBuilder sqlBuilder = new StringBuilder("CREATE SCHEMA \"" + schema + "\";");
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append("COMMENT ON SCHEMA ")
          .append(PG_QUOTE)
          .append(schema)
          .append(PG_QUOTE)
          .append(" IS E'")
          .append(escapeSqlLiteral(comment, '\''))
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
   * <p>The catalog parameter is sourced from {@link Connection#getCatalog()} rather than the
   * configured database field, because PostgreSQL JDBC driver 42.7+ strictly matches the catalog
   * parameter against the connection's actual catalog. In PostgreSQL, a connection can only connect
   * to a single database, and {@code setCatalog()} is effectively a no-op.
   *
   * @param connection the connection to the database
   * @param schemaName the name of the schema
   */
  private ResultSet getSchema(Connection connection, String schemaName) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getSchemas(connection.getCatalog(), schemaName);
  }

  @Override
  public String generateDropDatabaseSql(String schema, boolean cascade) {
    StringBuilder sqlBuilder =
        new StringBuilder(String.format("DROP SCHEMA %s%s%s", PG_QUOTE, schema, PG_QUOTE));
    if (cascade) {
      sqlBuilder.append(" CASCADE");
    }
    return sqlBuilder.toString();
  }

  @Override
  protected void dropDatabase(String schema, boolean cascade) {
    if (!cascade) {
      super.dropDatabase(schema, false);
      return;
    }
    try (Connection connection = getConnection()) {
      connection.setAutoCommit(false);
      try {
        // Keep existing relations stable while checking dependencies and executing the drop.
        try (PreparedStatement relations =
            connection.prepareStatement(
                "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                    + "ON c.relnamespace = n.oid WHERE n.nspname = ? AND c.relkind IN ('r', 'p') ORDER BY c.oid")) {
          relations.setString(1, schema);
          try (ResultSet result = relations.executeQuery()) {
            while (result.next()) {
              JdbcConnectorUtils.executeUpdate(
                  connection,
                  "LOCK TABLE ONLY "
                      + quoteName(schema)
                      + "."
                      + quoteName(result.getString(1))
                      + " IN ACCESS EXCLUSIVE MODE");
            }
          }
        }
        try (PreparedStatement statement = connection.prepareStatement(EXTERNAL_DEPENDENTS_SQL)) {
          statement.setString(1, schema);
          statement.setString(2, schema);
          try (ResultSet result = statement.executeQuery()) {
            List<String> external = new ArrayList<>();
            while (result.next()) {
              external.add(result.getString(1));
            }
            if (!external.isEmpty()) {
              throw new NonEmptySchemaException(
                  "Cannot drop schema %s: objects outside the schema depend on it: %s",
                  schema, external);
            }
          }
        }
        JdbcConnectorUtils.executeUpdate(connection, generateDropDatabaseSql(schema, true));
        connection.commit();
      } catch (SQLException | RuntimeException e) {
        try {
          connection.rollback();
        } catch (SQLException rollbackFailure) {
          e.addSuppressed(rollbackFailure);
        }
        throw e;
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  private static String quoteName(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
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
    return ImmutableSet.of("pg_toast", "pg_catalog", "information_schema");
  }

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
