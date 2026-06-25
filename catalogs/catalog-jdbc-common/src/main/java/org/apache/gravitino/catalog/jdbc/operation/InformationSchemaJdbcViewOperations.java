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
import java.sql.SQLException;
import org.apache.gravitino.exceptions.NoSuchViewException;

/**
 * View read operations that load view definitions from {@code information_schema.views} and use a
 * caller-provided dialect and quoting rules.
 */
public class InformationSchemaJdbcViewOperations extends JdbcViewOperations {

  private final String sqlDialect;
  private final String identifierQuote;

  /**
   * Creates view operations that use {@code information_schema.views}.
   *
   * @param sqlDialect The SQL dialect identifier for loaded views.
   * @param identifierQuote The quote character wrapped around identifiers.
   */
  public InformationSchemaJdbcViewOperations(String sqlDialect, String identifierQuote) {
    this.sqlDialect = sqlDialect;
    this.identifierQuote = identifierQuote;
  }

  @Override
  protected String getSqlDialect() {
    return sqlDialect;
  }

  @Override
  protected String quoteIdentifier(String identifier) {
    return identifierQuote + identifier + identifierQuote;
  }

  @Override
  protected String loadViewDefinition(Connection connection, String databaseName, String viewName)
      throws SQLException, NoSuchViewException {
    return loadViewDefinitionFromInformationSchema(connection, databaseName, viewName);
  }
}
