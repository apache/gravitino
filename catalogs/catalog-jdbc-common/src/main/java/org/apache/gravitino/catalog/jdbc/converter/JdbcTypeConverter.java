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
package org.apache.gravitino.catalog.jdbc.converter;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.gravitino.connector.DataTypeConverter;

public abstract class JdbcTypeConverter
    implements DataTypeConverter<String, JdbcTypeConverter.JdbcTypeBean> {

  public static final String DATE = "date";
  public static final String TIME = "time";
  public static final String TIMESTAMP = "timestamp";
  public static final String VARCHAR = "varchar";
  public static final String TEXT = "text";

  /**
   * Characters allowed outside single-quoted literals in an external type catalog string. Notably
   * excludes {@code ;}, {@code /}, {@code *}, {@code #} and newlines, so the string can neither
   * terminate the enclosing statement nor open a comment. The {@code --} line comment sequence is
   * rejected separately, because a single {@code -} is legitimate in type names such as {@code
   * user-defined}.
   */
  private static final Pattern EXTERNAL_TYPE_ALLOWED_CHARS =
      Pattern.compile("[\\w .,:()<>\\[\\]=-]*");

  /**
   * Validates that an external type catalog string is safe to interpolate into the column type
   * position of a generated DDL statement.
   *
   * <p>Catalog strings come from the caller and are written verbatim into the {@code CREATE TABLE}
   * and {@code ALTER TABLE} statements that the JDBC catalogs build by string concatenation. This
   * method deliberately does not check that the string names a type the backend understands, since
   * the backend rejects unknown types by itself. It only ensures the string stays within the single
   * type slot it is written into, so that it cannot append another comma separated {@code ALTER}
   * action, terminate the statement, or open a comment that hides the rest of the generated SQL.
   *
   * @param catalogString The external type catalog string to validate.
   * @return The unchanged {@code catalogString}, once it is known to be safe to interpolate.
   * @throws IllegalArgumentException if {@code catalogString} is null or blank, or if it could
   *     escape the column type position.
   */
  protected static String validateExternalTypeString(String catalogString) {
    Preconditions.checkArgument(
        catalogString != null && !catalogString.trim().isEmpty(),
        "External type cannot be null or blank, but got: %s",
        catalogString);

    String unquoted = stripSingleQuotedLiterals(catalogString);

    Preconditions.checkArgument(
        EXTERNAL_TYPE_ALLOWED_CHARS.matcher(unquoted).matches(),
        "External type contains characters that are not allowed outside a quoted literal: %s",
        catalogString);
    // Checked after the literals are stripped, so that a quoted literal is free to contain "--".
    // Two hyphens that only became adjacent because a literal between them was removed are
    // rejected as well, which errs on the safe side.
    Preconditions.checkArgument(
        !unquoted.contains("--"),
        "External type cannot contain the SQL line comment sequence '--': %s",
        catalogString);

    checkBracketsAndCommas(unquoted, catalogString);
    return catalogString;
  }

  /**
   * Removes every single-quoted literal from the given catalog string, so that the remaining text
   * can be checked without rejecting characters that are harmless inside a literal, such as the
   * members of a MySQL {@code enum('a','b')} declaration.
   *
   * @param catalogString The external type catalog string to strip.
   * @return The catalog string with the content of every quoted literal, and its quotes, removed.
   * @throws IllegalArgumentException if a quoted literal is never closed.
   */
  private static String stripSingleQuotedLiterals(String catalogString) {
    StringBuilder unquoted = new StringBuilder(catalogString.length());
    int index = 0;
    while (index < catalogString.length()) {
      char current = catalogString.charAt(index);
      if (current != '\'') {
        unquoted.append(current);
        index++;
        continue;
      }

      index++;
      boolean closed = false;
      while (index < catalogString.length()) {
        if (catalogString.charAt(index) != '\'') {
          index++;
        } else if (index + 1 < catalogString.length() && catalogString.charAt(index + 1) == '\'') {
          // A doubled quote escapes a quote inside the literal, it does not end the literal.
          index += 2;
        } else {
          index++;
          closed = true;
          break;
        }
      }
      Preconditions.checkArgument(
          closed, "External type has an unterminated quoted literal: %s", catalogString);
    }
    return unquoted.toString();
  }

  /**
   * Checks that {@code ()}, {@code []} and {@code <>} are balanced in the given quote stripped
   * text, and that every comma it contains is nested inside one of those pairs. A comma that is not
   * nested would start another action in the comma separated {@code ALTER TABLE} statements that
   * the JDBC catalogs generate.
   *
   * @param unquoted The catalog string with its quoted literals already stripped.
   * @param catalogString The original catalog string, used for the error message.
   * @throws IllegalArgumentException if a bracket pair is unbalanced or a comma is not nested.
   */
  private static void checkBracketsAndCommas(String unquoted, String catalogString) {
    int parentheses = 0;
    int squares = 0;
    int angles = 0;
    for (int index = 0; index < unquoted.length(); index++) {
      switch (unquoted.charAt(index)) {
        case '(':
          parentheses++;
          break;
        case ')':
          parentheses--;
          break;
        case '[':
          squares++;
          break;
        case ']':
          squares--;
          break;
        case '<':
          angles++;
          break;
        case '>':
          angles--;
          break;
        case ',':
          Preconditions.checkArgument(
              parentheses > 0 || squares > 0 || angles > 0,
              "External type cannot contain a comma outside of brackets: %s",
              catalogString);
          break;
        default:
          break;
      }

      Preconditions.checkArgument(
          parentheses >= 0 && squares >= 0 && angles >= 0,
          "External type closes a bracket that was never opened: %s",
          catalogString);
    }

    Preconditions.checkArgument(
        parentheses == 0 && squares == 0 && angles == 0,
        "External type has unbalanced brackets: %s",
        catalogString);
  }

  public static class JdbcTypeBean {
    /** Data type name. */
    private String typeName;

    /** Column size. For example: 20 in varchar (20) and 10 in decimal (10,2). */
    private Integer columnSize;

    /** Scale. For example: 2 in decimal (10,2). */
    private Integer scale;

    private Integer datetimePrecision;

    public JdbcTypeBean(String typeName) {
      this.typeName = typeName;
    }

    public String getTypeName() {
      return typeName;
    }

    public void setTypeName(String typeName) {
      this.typeName = typeName;
    }

    public Integer getColumnSize() {
      return columnSize;
    }

    public void setColumnSize(Integer columnSize) {
      this.columnSize = columnSize;
    }

    public Integer getScale() {
      return scale;
    }

    public void setScale(Integer scale) {
      this.scale = scale;
    }

    public Integer getDatetimePrecision() {
      return datetimePrecision;
    }

    public void setDatetimePrecision(Integer datetimePrecision) {
      this.datetimePrecision = datetimePrecision;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof JdbcTypeBean)) return false;
      JdbcTypeBean typeBean = (JdbcTypeBean) o;
      return Objects.equals(typeName, typeBean.typeName)
          && Objects.equals(columnSize, typeBean.columnSize)
          && Objects.equals(scale, typeBean.scale)
          && Objects.equals(datetimePrecision, typeBean.datetimePrecision);
    }

    @Override
    public int hashCode() {
      return Objects.hash(typeName, columnSize, scale, datetimePrecision);
    }

    @Override
    public String toString() {
      return "JdbcTypeBean{"
          + "typeName='"
          + typeName
          + '\''
          + ", columnSize='"
          + columnSize
          + '\''
          + ", scale='"
          + scale
          + '\''
          + ", datetimePrecision='"
          + datetimePrecision
          + '\''
          + '}';
    }
  }
}
