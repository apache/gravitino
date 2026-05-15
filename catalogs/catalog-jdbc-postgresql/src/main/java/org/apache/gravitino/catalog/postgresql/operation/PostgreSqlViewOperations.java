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

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.rel.Dialects;

/** PostgreSQL-specific implementation of JDBC view operations. */
public class PostgreSqlViewOperations extends JdbcViewOperations {

  private String database;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter typeConverter,
      Map<String, String> conf) {
    super.initialize(dataSource, exceptionMapper, typeConverter, conf);
    this.database = new JdbcConfig(conf).getJdbcDatabase();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(database),
        "The `jdbc-database` configuration item is mandatory in PostgreSQL.");
  }

  @Override
  protected String dialectName() {
    return Dialects.POSTGRESQL;
  }

  @Override
  protected String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  @Override
  protected String generateListViewsSql() {
    return "SELECT table_name FROM information_schema.views"
        + " WHERE table_schema = ? AND table_catalog = ?";
  }

  @Override
  protected void bindListViewsParameters(PreparedStatement stmt, String schemaName)
      throws SQLException {
    stmt.setString(1, schemaName);
    stmt.setString(2, database);
  }

  @Override
  protected String generateLoadViewSql() {
    return "SELECT view_definition FROM information_schema.views"
        + " WHERE table_schema = ? AND table_name = ? AND table_catalog = ?";
  }

  @Override
  protected void bindLoadViewParameters(PreparedStatement stmt, String schemaName, String viewName)
      throws SQLException {
    stmt.setString(1, schemaName);
    stmt.setString(2, viewName);
    stmt.setString(3, database);
  }

  @Override
  protected String generateCreateViewSql(String viewName, String sql) {
    return "CREATE VIEW " + quoteIdentifier(viewName) + " AS " + sql;
  }

  @Override
  protected String generateReplaceViewSql(String viewName, String sql) {
    return "CREATE OR REPLACE VIEW " + quoteIdentifier(viewName) + " AS " + sql;
  }

  @Override
  protected String generateRenameViewSql(String oldName, String newName) {
    return "ALTER VIEW " + quoteIdentifier(oldName) + " RENAME TO " + quoteIdentifier(newName);
  }

  @Override
  protected String generateDropViewSql(String viewName) {
    return "DROP VIEW IF EXISTS " + quoteIdentifier(viewName);
  }

  @Override
  protected void setComment(Connection connection, String viewName, String comment)
      throws SQLException {
    if (comment == null) {
      return;
    }
    String sql = "COMMENT ON VIEW " + quoteIdentifier(viewName) + " IS ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, comment);
      stmt.execute();
    }
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }
}
