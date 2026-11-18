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
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.connector.capability.CapabilityResult;

public final class JdbcConnectorUtils {
  public static final ImmutableList<String> TABLE_TYPES = ImmutableList.of("TABLE");

  private JdbcConnectorUtils() {}

  /**
   * Checks an existing native table name for catalogs whose table DDL safely quotes identifiers.
   * Creation rules still apply to schema names and new table names in the caller's capability. Dots
   * remain unsupported because Gravitino metadata objects cannot represent them in a name.
   *
   * @param name the unquoted existing table name
   * @return unsupported for empty, dot-containing, NUL-containing, or metadata-wildcard names
   */
  public static CapabilityResult existingQuotedTableName(String name) {
    if (name.isEmpty()
        || name.indexOf('.') >= 0
        || name.indexOf('\0') >= 0
        || MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(name)) {
      return CapabilityResult.unsupported(
          "An existing table name must be nonempty, contain no dot or NUL, and not be the metadata wildcard");
    }
    return CapabilityResult.SUPPORTED;
  }

  /**
   * Checks an existing native column name for catalogs whose column operations safely quote it. A
   * literal dot is permitted in a column reference; it is not a metadata object name.
   *
   * @param name the unquoted existing column name
   * @return unsupported for empty, NUL-containing, or metadata-wildcard names
   */
  public static CapabilityResult existingQuotedColumnName(String name) {
    if (name.isEmpty()
        || name.indexOf('\0') >= 0
        || MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(name)) {
      return CapabilityResult.unsupported(
          "An existing column name must be nonempty, contain no NUL, and not be the metadata wildcard");
    }
    return CapabilityResult.SUPPORTED;
  }

  /**
   * Quotes one identifier using a symmetric SQL identifier delimiter. Embedded delimiters are
   * doubled; backslashes are preserved. This is not a string-literal quoting helper.
   *
   * @param identifier the identifier, without surrounding delimiters
   * @param quote a backtick or double quote
   * @return the delimited identifier
   * @throws IllegalArgumentException if the delimiter is unsupported
   */
  public static String quoteIdentifier(String identifier, char quote) {
    if (quote != '`' && quote != '"') {
      throw new IllegalArgumentException("Identifier quote must be a backtick or double quote");
    }
    String delimiter = String.valueOf(quote);
    return delimiter + identifier.replace(delimiter, delimiter + delimiter) + delimiter;
  }

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
