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
package org.apache.gravitino.catalog.mysql.operation;

import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.rel.Dialects;

/** MySQL-specific implementation of JDBC view operations. */
public class MysqlViewOperations extends JdbcViewOperations {

  @Override
  public String dialectName() {
    return Dialects.MYSQL;
  }

  @Override
  protected String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  @Override
  protected String generateListViewsSql() {
    return "SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?";
  }

  @Override
  protected String generateLoadViewSql() {
    return "SELECT VIEW_DEFINITION FROM information_schema.VIEWS"
        + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
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
    return "RENAME TABLE " + quoteIdentifier(oldName) + " TO " + quoteIdentifier(newName);
  }

  @Override
  protected String generateDropViewSql(String viewName) {
    return "DROP VIEW " + quoteIdentifier(viewName);
  }
}
