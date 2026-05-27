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
package org.apache.gravitino.catalog.jdbc.operation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** SQLite-specific implementation of JDBC view operations for testing template methods. */
public class SqliteViewOperations extends JdbcViewOperations {

  @Override
  public String dialectName() {
    return "sqlite";
  }

  @Override
  protected String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  @Override
  protected String generateListViewsSql() {
    return "SELECT name FROM sqlite_master WHERE type = 'view' AND name NOT LIKE 'sqlite_%'";
  }

  @Override
  protected void bindListViewsParameters(PreparedStatement stmt, String databaseName)
      throws SQLException {
    // SQLite list-views query has no parameters
  }

  @Override
  protected String generateLoadViewSql() {
    return "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = ?";
  }

  @Override
  protected void bindLoadViewParameters(
      PreparedStatement stmt, String databaseName, String viewName) throws SQLException {
    stmt.setString(1, viewName);
  }

  @Override
  protected Connection getConnection(String databaseName) throws SQLException {
    return dataSource.getConnection();
  }
}
