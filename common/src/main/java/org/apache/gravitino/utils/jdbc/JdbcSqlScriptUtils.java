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

package org.apache.gravitino.utils.jdbc;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utility methods for parsing and executing SQL scripts. */
public final class JdbcSqlScriptUtils {

  private JdbcSqlScriptUtils() {}

  /**
   * Splits SQL script content into executable statements.
   *
   * <p>Lines starting with {@code --} are treated as comments and removed before splitting.
   *
   * @param sqlContent SQL script content
   * @return executable SQL statements in order
   */
  public static List<String> splitStatements(String sqlContent) {
    Preconditions.checkArgument(sqlContent != null, "sqlContent must not be null");
    String executableSql =
        Arrays.stream(sqlContent.split("\\R"))
            .map(String::trim)
            .filter(line -> !line.startsWith("--"))
            .collect(Collectors.joining("\n"));
    return Arrays.stream(executableSql.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }

  /**
   * Executes all SQL statements parsed from the script content.
   *
   * @param connection JDBC connection used to execute statements
   * @param sqlContent SQL script content
   * @throws SQLException if statement execution fails
   */
  public static void executeSqlScript(Connection connection, String sqlContent)
      throws SQLException {
    List<String> statements = splitStatements(sqlContent);
    try (Statement statement = connection.createStatement()) {
      for (String ddl : statements) {
        statement.execute(ddl);
      }
    }
  }
}
