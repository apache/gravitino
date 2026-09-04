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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.doris.spark.catalog.DorisTableCatalog;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.SchemaConvertors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.immutable.Map$;

/** Spark 3.5 bridge around the official Doris table catalog and Spark JDBC V2. */
// The Doris catalog base is compiled from Scala and exposes deprecated Spark bridge overrides.
@SuppressWarnings("overrides")
final class DorisTableCatalog35 extends DorisTableCatalog {

  private static final String LOAD_COLUMN_METADATA_SQL =
      "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns "
          + "WHERE table_schema = ? AND table_name = ? ORDER BY ORDINAL_POSITION";

  DorisPhysicalSchema35 loadPhysicalSchema(
      Identifier identifier,
      String jdbcUrl,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword) {
    if (identifier.namespace().length != 1) {
      throw new IllegalArgumentException("Doris table identifiers require one schema");
    }
    Schema schema;
    List<JdbcColumnMetadata> jdbcColumns;
    try {
      schema = frontend().getTableSchema(identifier.namespace()[0], identifier.name());
      jdbcColumns = loadJdbcColumnMetadata(identifier, jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword);
    } catch (Exception e) {
      throw physicalSchemaLoadFailure(identifier, e);
    }
    return buildPhysicalSchema(schema, jdbcColumns);
  }

  static DorisPhysicalSchema35 buildPhysicalSchema(
      Schema schema, List<JdbcColumnMetadata> jdbcColumns) {
    if (schema.size() != jdbcColumns.size()) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Doris FE and JDBC column counts differ: FE=%d, JDBC=%d",
              schema.size(),
              jdbcColumns.size()));
    }
    List<StructField> fields = new ArrayList<>(schema.size());
    List<String> typeNames = new ArrayList<>(schema.size());
    List<Boolean> catalystTypesResolved = new ArrayList<>(schema.size());
    List<Boolean> nullabilityKnown = new ArrayList<>(schema.size());
    for (int index = 0; index < schema.size(); index++) {
      Field field = schema.getProperties().get(index);
      JdbcColumnMetadata jdbcColumn = jdbcColumns.get(index);
      if (!field.getName().equalsIgnoreCase(jdbcColumn.name)
          || !samePhysicalTypeFamily(field.getType(), jdbcColumn.typeName)) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Doris FE and JDBC column metadata differ at index %d: FE=%s %s, JDBC=%s %s",
                index,
                field.getName(),
                field.getType(),
                jdbcColumn.name,
                jdbcColumn.typeName));
      }
      DataType dataType;
      boolean catalystTypeResolved;
      try {
        dataType =
            SchemaConvertors.toCatalystType(
                field.getType(), field.getPrecision(), field.getScale());
        catalystTypeResolved = true;
      } catch (Exception e) {
        // An unrecognized FE type must be carried as unresolved metadata so the compatibility
        // planner can fail closed using the exact JDBC COLUMN_TYPE.
        dataType = DataTypes.StringType;
        catalystTypeResolved = false;
      }
      fields.add(DataTypes.createStructField(field.getName(), dataType, jdbcColumn.nullable));
      typeNames.add(jdbcColumn.typeName);
      catalystTypesResolved.add(catalystTypeResolved);
      nullabilityKnown.add(true);
    }
    return new DorisPhysicalSchema35(
        DataTypes.createStructType(fields), typeNames, catalystTypesResolved, nullabilityKnown);
  }

  static IllegalArgumentException physicalSchemaLoadFailure(
      Identifier identifier, Throwable failure) {
    return new IllegalArgumentException(
        String.format(
            Locale.ROOT,
            "Failed to load Doris physical schema for %s (%s)",
            identifier,
            safeFailureDetails(failure)));
  }

  Table createJdbcTable(
      Identifier identifier,
      DorisReadSchema35 readSchema,
      String jdbcUrl,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword,
      DorisJdbcReadOptions35 readOptions) {
    return new JDBCTable(
        identifier,
        readSchema.schema(),
        jdbcOptions(
            jdbcUrl,
            readSchema.tableOrQuery(identifier),
            jdbcDriver,
            jdbcUser,
            jdbcPassword,
            readOptions));
  }

  private static List<JdbcColumnMetadata> loadJdbcColumnMetadata(
      Identifier identifier,
      String jdbcUrl,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword)
      throws Exception {
    Class.forName(jdbcDriver);
    List<JdbcColumnMetadata> columns = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        PreparedStatement statement = connection.prepareStatement(LOAD_COLUMN_METADATA_SQL)) {
      statement.setString(1, identifier.namespace()[0]);
      statement.setString(2, identifier.name());
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          columns.add(
              new JdbcColumnMetadata(
                  resultSet.getString(1),
                  resultSet.getString(2),
                  "YES".equalsIgnoreCase(resultSet.getString(3))));
        }
      }
    }
    return columns;
  }

  private static boolean samePhysicalTypeFamily(String frontendType, String jdbcType) {
    String frontendFamily = physicalTypeFamily(frontendType);
    String jdbcFamily = physicalTypeFamily(jdbcType);
    return !frontendFamily.isEmpty() && frontendFamily.equals(jdbcFamily);
  }

  private static String safeFailureDetails(Throwable failure) {
    String failureType = failure.getClass().getSimpleName();
    if (!(failure instanceof SQLException)) {
      return failureType;
    }
    SQLException sqlFailure = (SQLException) failure;
    String sqlState = sqlFailure.getSQLState();
    boolean safeSqlState =
        sqlState != null
            && sqlState.length() == 5
            && sqlState.chars().allMatch(Character::isLetterOrDigit);
    return String.format(
        Locale.ROOT,
        "%s, SQLState=%s, errorCode=%d",
        failureType,
        safeSqlState ? sqlState : "unknown",
        sqlFailure.getErrorCode());
  }

  private static String physicalTypeFamily(String typeName) {
    if (typeName == null) {
      return "";
    }
    String normalized = typeName.trim().toLowerCase(Locale.ROOT);
    int parameters = normalized.indexOf('(');
    int children = normalized.indexOf('<');
    int end = normalized.length();
    if (parameters >= 0) {
      end = Math.min(end, parameters);
    }
    if (children >= 0) {
      end = Math.min(end, children);
    }
    String family = normalized.substring(0, end).trim();
    if (family.startsWith("datetime")) {
      return "datetime";
    }
    if (family.equals("datev2")) {
      return "date";
    }
    if (family.startsWith("decimal")) {
      return "decimal";
    }
    if (family.equals("string") || family.equals("text")) {
      return "string";
    }
    return family;
  }

  private static JDBCOptions jdbcOptions(
      String jdbcUrl,
      String tableOrQuery,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword,
      DorisJdbcReadOptions35 readOptions) {
    scala.collection.immutable.Map<String, String> parameters = Map$.MODULE$.empty();
    parameters = parameters.$plus(new Tuple2<>("driver", jdbcDriver));
    parameters = parameters.$plus(new Tuple2<>("user", jdbcUser));
    parameters = parameters.$plus(new Tuple2<>("password", jdbcPassword));
    for (Map.Entry<String, String> entry : readOptions.asSparkOptions().entrySet()) {
      parameters = parameters.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return new JDBCOptions(jdbcUrl, tableOrQuery, parameters);
  }

  static final class JdbcColumnMetadata {
    private final String name;
    private final String typeName;
    private final boolean nullable;

    JdbcColumnMetadata(String name, String typeName, boolean nullable) {
      this.name = name;
      this.typeName = typeName;
      this.nullable = nullable;
    }
  }
}
