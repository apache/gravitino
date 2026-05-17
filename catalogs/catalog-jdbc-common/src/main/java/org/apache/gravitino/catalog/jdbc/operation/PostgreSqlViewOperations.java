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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Dialects;

/** View read operations for PostgreSQL and PostgreSQL-compatible catalogs. */
public class PostgreSqlViewOperations extends JdbcViewOperations {

  private static final String PG_QUOTE = "\"";

  @Override
  protected String getSqlDialect() {
    return Dialects.POSTGRESQL;
  }

  @Override
  protected String quoteIdentifier(String identifier) {
    return PG_QUOTE + identifier + PG_QUOTE;
  }

  @Override
  protected boolean matchesDatabase(ResultSet views, String databaseName) throws SQLException {
    return Objects.equals(views.getString("TABLE_SCHEM"), databaseName);
  }

  @Override
  protected Connection getConnection(String databaseName) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setSchema(databaseName);
    return connection;
  }

  @Override
  protected String loadViewDefinition(Connection connection, String databaseName, String viewName)
      throws SQLException, NoSuchViewException {
    String sql = "SELECT definition FROM pg_views WHERE schemaname = ? AND viewname = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, databaseName);
      statement.setString(2, viewName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchViewException("View %s does not exist in %s.", viewName, databaseName);
        }
        String definition = resultSet.getString("definition");
        if (StringUtils.isBlank(definition)) {
          throw new NoSuchViewException(
              "View definition for %s in %s is empty.", viewName, databaseName);
        }
        return definition.trim();
      }
    }
  }
}
