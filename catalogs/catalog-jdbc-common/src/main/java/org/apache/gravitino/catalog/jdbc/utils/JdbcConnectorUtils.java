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
package org.apache.gravitino.catalog.jdbc.utils;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nullable;

public final class JdbcConnectorUtils {
  public static final ImmutableList<String> TABLE_TYPES = ImmutableList.of("TABLE");

  private JdbcConnectorUtils() {}

  /**
   * Execute a SQL update statement against the given datasource.
   *
   * @param connection The connection to attempt to execute an update against
   * @param sql The sql to execute
   * @return The number of rows updated or exception
   * @throws SQLException on error during execution of the update to the underlying SQL data store
   */
  public static int executeUpdate(final Connection connection, final String sql)
      throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      return statement.executeUpdate(sql);
    }
  }

  /**
   * Escapes a value for inclusion in a quoted SQL string literal.
   *
   * <p>This method escapes both backslashes and the selected quote character. Callers are
   * responsible for adding the surrounding quote characters and any dialect-specific literal
   * prefix.
   *
   * @param value the literal value to escape; {@code null} is represented as the text {@code null}
   * @param quote the quote character used by the SQL dialect
   * @return the escaped literal value
   * @throws IllegalArgumentException if {@code quote} is neither a single nor double quote
   */
  public static String escapeSqlLiteral(@Nullable String value, char quote) {
    if (quote != '\'' && quote != '"') {
      throw new IllegalArgumentException("SQL literal quote must be a single or double quote");
    }

    String literalValue = String.valueOf(value);
    String quoteString = String.valueOf(quote);
    return literalValue.replace("\\", "\\\\").replace(quoteString, quoteString + quoteString);
  }

  /**
   * Reverses {@link #escapeSqlLiteral(String, char)} for text captured from a quoted SQL string
   * literal, e.g. when parsing {@code SHOW CREATE} output.
   *
   * <p>Both doubled-quote ({@code ""} or {@code ''}) and backslash ({@code \"}, {@code \\}) escape
   * styles are unescaped, since dialects differ in which form they emit. Any other character is
   * kept as is, so text that was never escaped passes through unchanged.
   *
   * @param value the captured literal text without the surrounding quote characters
   * @param quote the quote character used by the SQL dialect
   * @return the unescaped literal value
   * @throws IllegalArgumentException if {@code quote} is neither a single nor double quote
   */
  public static String unescapeSqlLiteral(String value, char quote) {
    if (quote != '\'' && quote != '"') {
      throw new IllegalArgumentException("SQL literal quote must be a single or double quote");
    }

    StringBuilder result = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char current = value.charAt(i);
      if (current == '\\' && i + 1 < value.length()) {
        char next = value.charAt(i + 1);
        if (next == '\\' || next == quote) {
          result.append(next);
          i++;
          continue;
        }
      } else if (current == quote && i + 1 < value.length() && value.charAt(i + 1) == quote) {
        result.append(quote);
        i++;
        continue;
      }
      result.append(current);
    }
    return result.toString();
  }

  public static String[] getTableTypes() {
    return TABLE_TYPES.toArray(new String[0]);
  }
}
