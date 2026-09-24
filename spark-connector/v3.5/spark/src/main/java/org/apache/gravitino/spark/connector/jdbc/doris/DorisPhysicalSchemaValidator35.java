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

package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Validates one authorized Doris table against FE and JDBC physical metadata. */
final class DorisPhysicalSchemaValidator35 {

  private static final Set<String> ORDINARY_TYPES =
      ImmutableSet.of(
          "bigint",
          "boolean",
          "char",
          "date",
          "datev2",
          "datetime",
          "datetimev2",
          "decimal",
          "double",
          "float",
          "int",
          "integer",
          "smallint",
          "string",
          "text",
          "tinyint",
          "varchar");

  private DorisPhysicalSchemaValidator35() {}

  static void validate(
      Identifier identifier,
      Table logicalTable,
      StructType sparkPhysicalSchema,
      String jdbcUrl,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword,
      SparkTypeConverter typeConverter) {
    try {
      Class.forName(jdbcDriver);
      try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
        List<PhysicalColumn> jdbcColumns = loadJdbcColumns(connection, identifier);
        List<PhysicalColumn> feColumns = loadFeColumns(connection, identifier);
        validateColumns(
            identifier, logicalTable, sparkPhysicalSchema, jdbcColumns, feColumns, typeConverter);
      }
    } catch (ClassNotFoundException | SQLException | RuntimeException e) {
      if (e instanceof IllegalArgumentException
          && e.getMessage() != null
          && e.getMessage().startsWith("Doris schema mismatch")) {
        throw (IllegalArgumentException) e;
      }
      throw new IllegalArgumentException(
          "Doris physical schema validation failed for " + identifier);
    }
  }

  private static List<PhysicalColumn> loadJdbcColumns(Connection connection, Identifier identifier)
      throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    List<PhysicalColumn> columns = new ArrayList<>();
    try (ResultSet resultSet =
        metadata.getColumns(null, identifier.namespace()[0], identifier.name(), "%")) {
      while (resultSet.next()) {
        String typeName = resultSet.getString("TYPE_NAME");
        columns.add(
            new PhysicalColumn(
                resultSet.getString("COLUMN_NAME"),
                typeName,
                nullableValue(resultSet.getInt("NULLABLE")),
                resultSet.getInt("ORDINAL_POSITION"),
                typeSignature(
                    typeName,
                    nullableInt(resultSet, "COLUMN_SIZE"),
                    nullableInt(resultSet, "DECIMAL_DIGITS"))));
      }
    }
    columns.sort(Comparator.comparingInt(PhysicalColumn::ordinal));
    return columns;
  }

  private static List<PhysicalColumn> loadFeColumns(Connection connection, Identifier identifier)
      throws SQLException {
    String database = quote(identifier.namespace()[0]);
    String table = quote(identifier.name());
    List<PhysicalColumn> columns = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SHOW COLUMNS FROM " + database + "." + table)) {
      int ordinal = 0;
      while (resultSet.next()) {
        String typeName = resultSet.getString(2);
        columns.add(
            new PhysicalColumn(
                resultSet.getString(1),
                typeName,
                nullableValue(resultSet.getString(3)),
                ordinal++,
                typeSignature(typeName, null, null)));
      }
    }
    return columns;
  }

  static void validateColumns(
      Identifier identifier,
      Table logicalTable,
      StructType sparkPhysicalSchema,
      List<PhysicalColumn> jdbcColumns,
      List<PhysicalColumn> feColumns,
      SparkTypeConverter typeConverter) {
    Column[] logicalColumns = logicalTable.columns();
    StructField[] sparkColumns = sparkPhysicalSchema.fields();
    if (logicalColumns.length != sparkColumns.length
        || logicalColumns.length != jdbcColumns.size()
        || logicalColumns.length != feColumns.size()) {
      throw mismatch(
          identifier, "column counts differ across logical, FE, JDBC, and Spark schemas");
    }

    Set<String> names = new HashSet<>();
    for (int index = 0; index < logicalColumns.length; index++) {
      Column logical = logicalColumns[index];
      StructField spark = sparkColumns[index];
      PhysicalColumn jdbc = jdbcColumns.get(index);
      PhysicalColumn fe = feColumns.get(index);
      if (!names.add(logical.name().toLowerCase(Locale.ROOT))) {
        throw mismatch(identifier, "duplicate logical column names");
      }
      if (!logical.name().equalsIgnoreCase(spark.name())
          || !logical.name().equalsIgnoreCase(jdbc.name())
          || !logical.name().equalsIgnoreCase(fe.name())) {
        throw mismatch(identifier, "column names or order differ");
      }
      boolean sameTypeFamily = baseType(jdbc.typeName()).equals(baseType(fe.typeName()));
      boolean sameTypeSignature = jdbc.signature().equals(fe.signature());
      if (!sameTypeFamily && !sameTypeSignature) {
        throw mismatch(identifier, "FE and JDBC type families differ for " + logical.name());
      }
      if (!sameTypeSignature) {
        throw mismatch(identifier, "FE and JDBC type signatures differ for " + logical.name());
      }
      if (!ORDINARY_TYPES.contains(baseType(fe.typeName()))) {
        throw mismatch(identifier, "unsupported Doris type for " + logical.name());
      }
      DataType expected = typeConverter.toSparkType(logical.dataType());
      if (!expected.equals(spark.dataType())) {
        throw mismatch(identifier, "logical and Spark physical types differ for " + logical.name());
      }
      if (!jdbc.nullabilityKnown() || !fe.nullabilityKnown()) {
        throw mismatch(identifier, "FE or JDBC nullability is unknown for " + logical.name());
      }
      if (!Objects.equals(jdbc.nullable(), fe.nullable())) {
        throw mismatch(identifier, "FE and JDBC nullability differs for " + logical.name());
      }
      if (logical.nullable() != fe.nullable()) {
        throw mismatch(
            identifier, "logical and physical nullability differs for " + logical.name());
      }
    }
  }

  private static String baseType(String typeName) {
    if (typeName == null) {
      return "";
    }
    String normalized = typeName.trim().toLowerCase(Locale.ROOT);
    int openingParenthesis = normalized.indexOf('(');
    return openingParenthesis < 0 ? normalized : normalized.substring(0, openingParenthesis).trim();
  }

  private static String typeSignature(String typeName, Integer size, Integer scale) {
    if (typeName == null) {
      return "";
    }
    String normalized = typeName.trim().toLowerCase(Locale.ROOT).replace(" ", "");
    String base = baseType(normalized);
    switch (base) {
      case "datev2":
        return "date";
      case "datetimev2":
        base = "datetime";
        break;
      case "decimalv3":
        base = "decimal";
        break;
      case "string":
      case "text":
      case "longvarchar":
        return "string";
      default:
        break;
    }

    String parameters = parameters(normalized);
    if (parameters.isEmpty() && size != null) {
      if ("decimal".equals(base) && scale != null) {
        parameters = "(" + size + "," + scale + ")";
      } else if ("char".equals(base) || "varchar".equals(base)) {
        parameters = "(" + size + ")";
      }
    }
    if (parameters.isEmpty() && "datetime".equals(base) && scale != null) {
      parameters = "(" + scale + ")";
    }
    return base + parameters;
  }

  private static String parameters(String typeName) {
    int openingParenthesis = typeName.indexOf('(');
    if (openingParenthesis < 0 || !typeName.endsWith(")")) {
      return "";
    }
    return typeName.substring(openingParenthesis);
  }

  private static Integer nullableInt(ResultSet resultSet, String column) throws SQLException {
    int value = resultSet.getInt(column);
    return resultSet.wasNull() ? null : value;
  }

  private static String quote(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  private static Boolean nullableValue(int value) {
    if (value == DatabaseMetaData.columnNoNulls) {
      return Boolean.FALSE;
    }
    if (value == DatabaseMetaData.columnNullable) {
      return Boolean.TRUE;
    }
    return null;
  }

  private static Boolean nullableValue(String value) {
    if ("YES".equalsIgnoreCase(value)) {
      return Boolean.TRUE;
    }
    if ("NO".equalsIgnoreCase(value)) {
      return Boolean.FALSE;
    }
    return null;
  }

  private static IllegalArgumentException mismatch(Identifier identifier, String reason) {
    return new IllegalArgumentException("Doris schema mismatch for " + identifier + ": " + reason);
  }

  static final class PhysicalColumn {
    private final String name;
    private final String typeName;
    private final String signature;
    private final Boolean nullable;
    private final int ordinal;

    PhysicalColumn(String name, String typeName, Boolean nullable, int ordinal) {
      this(name, typeName, nullable, ordinal, typeSignature(typeName, null, null));
    }

    PhysicalColumn(
        String name, String typeName, Boolean nullable, int ordinal, String signature) {
      this.name = name;
      this.typeName = typeName;
      this.signature = signature;
      this.nullable = nullable;
      this.ordinal = ordinal;
    }

    String name() {
      return name;
    }

    String typeName() {
      return typeName;
    }

    String signature() {
      return signature;
    }

    Boolean nullable() {
      return nullable;
    }

    boolean nullabilityKnown() {
      return nullable != null;
    }

    int ordinal() {
      return ordinal;
    }
  }
}
