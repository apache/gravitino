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
package org.apache.gravitino.catalog.hologres.operation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;

/**
 * Table operations for Hologres.
 *
 * <p>Hologres is PostgreSQL-compatible, so most table operations follow PostgreSQL conventions.
 * However, Hologres has specific features like table properties (orientation, distribution_key,
 * etc.) that are handled through the WITH clause in CREATE TABLE statements.
 */
public class HologresTableOperations extends JdbcTableOperations
    implements RequireDatabaseOperation {

  public static final String HOLO_QUOTE = "\"";
  public static final String NEW_LINE = "\n";
  public static final String ALTER_TABLE = "ALTER TABLE ";
  public static final String ALTER_COLUMN = "ALTER COLUMN ";
  public static final String IS = " IS '";
  public static final String COLUMN_COMMENT = "COMMENT ON COLUMN ";
  public static final String TABLE_COMMENT = "COMMENT ON TABLE ";

  private static final String HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Hologres does not support nested column names.";

  private String database;
  private HologresSchemaOperations schemaOperations;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf) {
    super.initialize(
        dataSource, exceptionMapper, jdbcTypeConverter, jdbcColumnDefaultValueConverter, conf);
    database = new JdbcConfig(conf).getJdbcDatabase();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(database),
        "The `jdbc-database` configuration item is mandatory in Hologres.");
  }

  @Override
  public void setDatabaseOperation(DatabaseOperation databaseOperation) {
    this.schemaOperations = (HologresSchemaOperations) databaseOperation;
  }

  @Override
  public List<String> listTables(String schemaName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(schemaName)) {
      if (!schemaOperations.schemaExists(connection, schemaName)) {
        throw new NoSuchSchemaException("No such schema: %s", schemaName);
      }
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        while (tables.next()) {
          if (Objects.equals(tables.getString("TABLE_SCHEM"), schemaName)) {
            names.add(tables.getString("TABLE_NAME"));
          }
        }
      }
      LOG.info("Finished listing tables size {} for schema name {} ", names.size(), schemaName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected JdbcTable.Builder getTableBuilder(
      ResultSet tablesResult, String databaseName, String tableName) throws SQLException {
    boolean found = false;
    JdbcTable.Builder builder = null;
    while (tablesResult.next() && !found) {
      String tableNameInResult = tablesResult.getString("TABLE_NAME");
      String tableSchemaInResultLowerCase = tablesResult.getString("TABLE_SCHEM");
      if (Objects.equals(tableNameInResult, tableName)
          && Objects.equals(tableSchemaInResultLowerCase, databaseName)) {
        builder = getBasicJdbcTableInfo(tablesResult);
        found = true;
      }
    }

    if (!found) {
      throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
    }

    return builder;
  }

  @Override
  protected JdbcColumn.Builder getColumnBuilder(
      ResultSet columnsResult, String databaseName, String tableName) throws SQLException {
    JdbcColumn.Builder builder = null;
    if (Objects.equals(columnsResult.getString("TABLE_NAME"), tableName)
        && Objects.equals(columnsResult.getString("TABLE_SCHEM"), databaseName)) {
      builder = getBasicJdbcColumnInfo(columnsResult);
    }
    return builder;
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    boolean isLogicalPartition =
        MapUtils.isNotEmpty(properties)
            && "true".equalsIgnoreCase(properties.get("is_logical_partitioned_table"));
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(HOLO_QUOTE)
        .append(tableName)
        .append(HOLO_QUOTE)
        .append(" (")
        .append(NEW_LINE);

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("    ").append(HOLO_QUOTE).append(column.name()).append(HOLO_QUOTE);

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",").append(NEW_LINE);
      }
    }
    appendIndexesSql(indexes, sqlBuilder);
    sqlBuilder.append(NEW_LINE).append(")");

    // Append partitioning clause if specified
    if (ArrayUtils.isNotEmpty(partitioning)) {
      appendPartitioningSql(partitioning, isLogicalPartition, sqlBuilder);
    }

    // Build WITH clause combining distribution and Hologres-specific table properties
    // Supported properties: orientation, distribution_key, clustering_key, event_time_column,
    // bitmap_columns, dictionary_encoding_columns, time_to_live_in_seconds, table_group, etc.
    List<String> withEntries = new ArrayList<>();

    // Add distribution_key from Distribution parameter
    if (!Distributions.NONE.equals(distribution)) {
      validateDistribution(distribution);
      String distributionColumns =
          Arrays.stream(distribution.expressions())
              .map(Object::toString)
              .collect(Collectors.joining(","));
      withEntries.add("distribution_key = '" + distributionColumns + "'");
    }

    // Add user-specified properties (filter out read-only / internally-handled properties)
    if (MapUtils.isNotEmpty(properties)) {
      properties.forEach(
          (key, value) -> {
            if (!"distribution_key".equals(key)
                && !"is_logical_partitioned_table".equals(key)
                && !"primary_key".equals(key)) {
              withEntries.add(key + " = '" + value + "'");
            }
          });
    }

    // Generate WITH clause
    if (!withEntries.isEmpty()) {
      sqlBuilder.append(NEW_LINE).append("WITH (").append(NEW_LINE);
      sqlBuilder.append(
          withEntries.stream()
              .map(entry -> "    " + entry)
              .collect(Collectors.joining("," + NEW_LINE)));
      sqlBuilder.append(NEW_LINE).append(")");
    }

    sqlBuilder.append(";");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append(NEW_LINE)
          .append(TABLE_COMMENT)
          .append(HOLO_QUOTE)
          .append(tableName)
          .append(HOLO_QUOTE)
          .append(IS)
          .append(comment.replace("'", "''"))
          .append("';");
    }
    Arrays.stream(columns)
        .filter(jdbcColumn -> StringUtils.isNotEmpty(jdbcColumn.comment()))
        .forEach(
            jdbcColumn ->
                sqlBuilder
                    .append(NEW_LINE)
                    .append(COLUMN_COMMENT)
                    .append(HOLO_QUOTE)
                    .append(tableName)
                    .append(HOLO_QUOTE)
                    .append(".")
                    .append(HOLO_QUOTE)
                    .append(jdbcColumn.name())
                    .append(HOLO_QUOTE)
                    .append(IS)
                    .append(jdbcColumn.comment().replace("'", "''"))
                    .append("';"));

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @VisibleForTesting
  static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",").append(NEW_LINE);
      switch (index.type()) {
        case PRIMARY_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sqlBuilder
                .append("CONSTRAINT ")
                .append(HOLO_QUOTE)
                .append(index.name())
                .append(HOLO_QUOTE);
          }
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sqlBuilder
                .append("CONSTRAINT ")
                .append(HOLO_QUOTE)
                .append(index.name())
                .append(HOLO_QUOTE);
          }
          sqlBuilder.append(" UNIQUE (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException("Hologres doesn't support index : " + index.type());
      }
    }
  }

  protected static String getIndexFieldStr(String[][] fieldNames) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in Hologres");
              }
              return HOLO_QUOTE + colNames[0] + HOLO_QUOTE;
            })
        .collect(Collectors.joining(", "));
  }

  /**
   * Append the partitioning clause to the CREATE TABLE SQL.
   *
   * <p>Hologres supports two types of partition tables:
   *
   * <ul>
   *   <li>Physical partition table: uses {@code PARTITION BY LIST(column)} syntax
   *   <li>Logical partition table (V3.1+): uses {@code LOGICAL PARTITION BY LIST(column1[,
   *       column2])} syntax
   * </ul>
   *
   * @param partitioning the partition transforms (only LIST partitioning is supported)
   * @param isLogicalPartition whether to create a logical partition table
   * @param sqlBuilder the SQL builder to append to
   */
  @VisibleForTesting
  static void appendPartitioningSql(
      Transform[] partitioning, boolean isLogicalPartition, StringBuilder sqlBuilder) {
    Preconditions.checkArgument(
        partitioning.length == 1 && partitioning[0] instanceof Transforms.ListTransform,
        "Hologres only supports LIST partitioning");

    Transforms.ListTransform listTransform = (Transforms.ListTransform) partitioning[0];
    String[][] fieldNames = listTransform.fieldNames();

    Preconditions.checkArgument(fieldNames.length > 0, "Partition columns must not be empty");

    if (isLogicalPartition) {
      Preconditions.checkArgument(
          fieldNames.length <= 2,
          "Logical partition table supports at most 2 partition columns, but got: %s",
          fieldNames.length);
    } else {
      Preconditions.checkArgument(
          fieldNames.length == 1,
          "Physical partition table supports exactly 1 partition column, but got: %s",
          fieldNames.length);
    }

    String partitionColumns =
        Arrays.stream(fieldNames)
            .map(
                colNames -> {
                  Preconditions.checkArgument(
                      colNames.length == 1,
                      "Hologres partition does not support nested field names");
                  return HOLO_QUOTE + colNames[0] + HOLO_QUOTE;
                })
            .collect(Collectors.joining(", "));

    sqlBuilder.append(NEW_LINE);
    if (isLogicalPartition) {
      sqlBuilder.append("LOGICAL PARTITION BY LIST(").append(partitionColumns).append(")");
    } else {
      sqlBuilder.append("PARTITION BY LIST(").append(partitionColumns).append(")");
    }
  }

  private void appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add data type
    sqlBuilder.append(SPACE).append(typeConverter.fromGravitino(column.dataType())).append(SPACE);

    if (column.autoIncrement()) {
      if (!Types.allowAutoIncrement(column.dataType())) {
        throw new IllegalArgumentException(
            "Unsupported auto-increment , column: "
                + column.name()
                + ", type: "
                + column.dataType());
      }
      sqlBuilder.append("GENERATED BY DEFAULT AS IDENTITY ");
    }

    // Add NOT NULL if the column is marked as such
    if (column.nullable()) {
      sqlBuilder.append("NULL ");
    } else {
      sqlBuilder.append("NOT NULL ");
    }
    // Add DEFAULT value if specified
    appendDefaultValue(column, sqlBuilder);
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return ALTER_TABLE
        + HOLO_QUOTE
        + oldTableName
        + HOLO_QUOTE
        + " RENAME TO "
        + HOLO_QUOTE
        + newTableName
        + HOLO_QUOTE;
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return "DROP TABLE " + HOLO_QUOTE + tableName + HOLO_QUOTE;
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "Hologres does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String schemaName, String tableName, TableChange... changes) {
    // Not all operations require the original table information, so lazy loading is used here
    JdbcTable lazyLoadTable = null;
    List<String> alterSql = new ArrayList<>();
    for (TableChange change : changes) {
      if (change instanceof TableChange.UpdateComment) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        alterSql.add(updateCommentDefinition((TableChange.UpdateComment) change, lazyLoadTable));
      } else if (change instanceof TableChange.SetProperty) {
        throw new IllegalArgumentException("Set property is not supported yet");
      } else if (change instanceof TableChange.RemoveProperty) {
        throw new IllegalArgumentException("Remove property is not supported yet");
      } else if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        alterSql.addAll(addColumnFieldDefinition(addColumn, lazyLoadTable));
      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        alterSql.add(renameColumnFieldDefinition(renameColumn, tableName));
      } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
            (TableChange.UpdateColumnDefaultValue) change;
        alterSql.add(
            updateColumnDefaultValueFieldDefinition(updateColumnDefaultValue, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnType) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnComment) {
        alterSql.add(
            updateColumnCommentFieldDefinition(
                (TableChange.UpdateColumnComment) change, tableName));
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        throw new IllegalArgumentException("Hologres does not support column position.");
      } else if (change instanceof TableChange.DeleteColumn) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
        String deleteColSql = deleteColumnFieldDefinition(deleteColumn, lazyLoadTable);
        if (StringUtils.isNotEmpty(deleteColSql)) {
          alterSql.add(deleteColSql);
        }
      } else if (change instanceof TableChange.UpdateColumnNullability) {
        TableChange.UpdateColumnNullability updateColumnNullability =
            (TableChange.UpdateColumnNullability) change;

        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        validateUpdateColumnNullable(updateColumnNullability, lazyLoadTable);

        alterSql.add(updateColumnNullabilityDefinition(updateColumnNullability, tableName));
      } else if (change instanceof TableChange.AddIndex) {
        alterSql.add(addIndexDefinition(tableName, (TableChange.AddIndex) change));
      } else if (change instanceof TableChange.DeleteIndex) {
        alterSql.add(deleteIndexDefinition(tableName, (TableChange.DeleteIndex) change));
      } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
        alterSql.add(
            updateColumnAutoIncrementDefinition(
                (TableChange.UpdateColumnAutoIncrement) change, tableName));
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type: " + change.getClass().getName());
      }
    }

    // If there is no change, return directly
    if (alterSql.isEmpty()) {
      return "";
    }

    // Return the generated SQL statement
    String result = String.join("\n", alterSql);
    LOG.info("Generated alter table:{}.{} sql: {}", schemaName, tableName, result);
    return result;
  }

  @VisibleForTesting
  static String updateColumnAutoIncrementDefinition(
      TableChange.UpdateColumnAutoIncrement change, String tableName) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String fieldName = change.fieldName()[0];
    String action =
        change.isAutoIncrement() ? "ADD GENERATED BY DEFAULT AS IDENTITY" : "DROP IDENTITY";

    return String.format(
        "ALTER TABLE %s %s %s %s;",
        HOLO_QUOTE + tableName + HOLO_QUOTE,
        ALTER_COLUMN,
        HOLO_QUOTE + fieldName + HOLO_QUOTE,
        action);
  }

  @VisibleForTesting
  static String deleteIndexDefinition(String tableName, TableChange.DeleteIndex deleteIndex) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("ALTER TABLE ")
        .append(HOLO_QUOTE)
        .append(tableName)
        .append(HOLO_QUOTE)
        .append(" DROP CONSTRAINT ")
        .append(HOLO_QUOTE)
        .append(deleteIndex.getName())
        .append(HOLO_QUOTE)
        .append(";\n");
    if (deleteIndex.isIfExists()) {
      sqlBuilder
          .append("DROP INDEX IF EXISTS ")
          .append(HOLO_QUOTE)
          .append(deleteIndex.getName())
          .append(HOLO_QUOTE)
          .append(";");
    } else {
      sqlBuilder
          .append("DROP INDEX ")
          .append(HOLO_QUOTE)
          .append(deleteIndex.getName())
          .append(HOLO_QUOTE)
          .append(";");
    }
    return sqlBuilder.toString();
  }

  @VisibleForTesting
  static String addIndexDefinition(String tableName, TableChange.AddIndex addIndex) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("ALTER TABLE ")
        .append(HOLO_QUOTE)
        .append(tableName)
        .append(HOLO_QUOTE)
        .append(" ADD CONSTRAINT ")
        .append(HOLO_QUOTE)
        .append(addIndex.getName())
        .append(HOLO_QUOTE);
    switch (addIndex.getType()) {
      case PRIMARY_KEY:
        sqlBuilder.append(" PRIMARY KEY ");
        break;
      case UNIQUE_KEY:
        sqlBuilder.append(" UNIQUE ");
        break;
      default:
        throw new IllegalArgumentException("Unsupported index type: " + addIndex.getType());
    }
    sqlBuilder.append("(").append(getIndexFieldStr(addIndex.getFieldNames())).append(");");
    return sqlBuilder.toString();
  }

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability updateColumnNullability, String tableName) {
    if (updateColumnNullability.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnNullability.fieldName()[0];
    if (updateColumnNullability.nullable()) {
      return ALTER_TABLE
          + HOLO_QUOTE
          + tableName
          + HOLO_QUOTE
          + " "
          + ALTER_COLUMN
          + HOLO_QUOTE
          + col
          + HOLO_QUOTE
          + " DROP NOT NULL;";
    } else {
      return ALTER_TABLE
          + HOLO_QUOTE
          + tableName
          + HOLO_QUOTE
          + " "
          + ALTER_COLUMN
          + HOLO_QUOTE
          + col
          + HOLO_QUOTE
          + " SET NOT NULL;";
    }
  }

  private String updateCommentDefinition(
      TableChange.UpdateComment updateComment, JdbcTable jdbcTable) {
    String newComment = updateComment.getNewComment();
    if (null == StringIdentifier.fromComment(newComment)) {
      // Detect and add Gravitino id.
      if (StringUtils.isNotEmpty(jdbcTable.comment())) {
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
    }
    return TABLE_COMMENT + HOLO_QUOTE + jdbcTable.name() + HOLO_QUOTE + IS + newComment + "';";
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable table) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = deleteColumn.fieldName()[0];
    boolean colExists =
        Arrays.stream(table.columns()).anyMatch(s -> StringUtils.equals(col, s.name()));
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column does not exist: " + col);
      }
    }
    return ALTER_TABLE
        + HOLO_QUOTE
        + table.name()
        + HOLO_QUOTE
        + " DROP COLUMN "
        + HOLO_QUOTE
        + deleteColumn.fieldName()[0]
        + HOLO_QUOTE
        + ";";
  }

  private String updateColumnDefaultValueFieldDefinition(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, JdbcTable jdbcTable) {
    if (updateColumnDefaultValue.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnDefaultValue.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column %s does not exist.", col);
    }

    StringBuilder sqlBuilder = new StringBuilder(ALTER_TABLE + jdbcTable.name());
    sqlBuilder
        .append("\n")
        .append(ALTER_COLUMN)
        .append(HOLO_QUOTE)
        .append(col)
        .append(HOLO_QUOTE)
        .append(" SET DEFAULT ")
        .append(
            columnDefaultValueConverter.fromGravitino(
                updateColumnDefaultValue.getNewDefaultValue()));
    return sqlBuilder.append(";").toString();
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column %s does not exist.", col);
    }
    StringBuilder sqlBuilder = new StringBuilder(ALTER_TABLE + jdbcTable.name());
    sqlBuilder
        .append("\n")
        .append(ALTER_COLUMN)
        .append(HOLO_QUOTE)
        .append(col)
        .append(HOLO_QUOTE)
        .append(" SET DATA TYPE ")
        .append(typeConverter.fromGravitino(updateColumnType.getNewDataType()));
    if (!column.nullable()) {
      sqlBuilder
          .append(",\n")
          .append(ALTER_COLUMN)
          .append(HOLO_QUOTE)
          .append(col)
          .append(HOLO_QUOTE)
          .append(" SET NOT NULL");
    }
    return sqlBuilder.append(";").toString();
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, String tableName) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    return ALTER_TABLE
        + tableName
        + " RENAME COLUMN "
        + HOLO_QUOTE
        + renameColumn.fieldName()[0]
        + HOLO_QUOTE
        + SPACE
        + "TO"
        + SPACE
        + HOLO_QUOTE
        + renameColumn.getNewName()
        + HOLO_QUOTE
        + ";";
  }

  private List<String> addColumnFieldDefinition(
      TableChange.AddColumn addColumn, JdbcTable lazyLoadTable) {
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    List<String> result = new ArrayList<>();
    String col = addColumn.fieldName()[0];

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition
        .append(ALTER_TABLE)
        .append(lazyLoadTable.name())
        .append(SPACE)
        .append("ADD COLUMN ")
        .append(HOLO_QUOTE)
        .append(col)
        .append(HOLO_QUOTE)
        .append(SPACE)
        .append(typeConverter.fromGravitino(addColumn.getDataType()))
        .append(SPACE);

    if (addColumn.isAutoIncrement()) {
      if (!Types.allowAutoIncrement(addColumn.getDataType())) {
        throw new IllegalArgumentException(
            "Unsupported auto-increment , column: "
                + Arrays.toString(addColumn.getFieldName())
                + ", type: "
                + addColumn.getDataType());
      }
      columnDefinition.append("GENERATED BY DEFAULT AS IDENTITY ");
    }

    // Add NOT NULL if the column is marked as such
    if (!addColumn.isNullable()) {
      columnDefinition.append("NOT NULL ");
    }

    // Append default value if available
    if (!Column.DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      columnDefinition
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue()))
          .append(SPACE);
    }

    // Append position if available
    if (!(addColumn.getPosition() instanceof TableChange.Default)) {
      throw new IllegalArgumentException("Hologres does not support column position in Gravitino.");
    }
    result.add(columnDefinition.append(";").toString());

    // Append comment if available
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      result.add(
          COLUMN_COMMENT
              + HOLO_QUOTE
              + lazyLoadTable.name()
              + HOLO_QUOTE
              + "."
              + HOLO_QUOTE
              + col
              + HOLO_QUOTE
              + IS
              + addColumn.getComment()
              + "';");
    }
    return result;
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, String tableName) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(HOLOGRES_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnComment.fieldName()[0];
    return COLUMN_COMMENT
        + HOLO_QUOTE
        + tableName
        + HOLO_QUOTE
        + "."
        + HOLO_QUOTE
        + col
        + HOLO_QUOTE
        + IS
        + newComment
        + "';";
  }

  @Override
  protected ResultSet getIndexInfo(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(database, schemaName, tableName, false, false);
  }

  @Override
  protected ResultSet getPrimaryKeys(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getPrimaryKeys(database, schemaName, tableName);
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }

  /**
   * Get tables from the database including regular tables and partitioned parent tables.
   *
   * <p>In Hologres (PostgreSQL-compatible):
   *
   * <ul>
   *   <li>Regular tables and partition child tables have TABLE_TYPE = "TABLE"
   *   <li>Partitioned parent tables have TABLE_TYPE = "PARTITIONED TABLE"
   *   <li>Views have TABLE_TYPE = "VIEW" (excluded from listing)
   *   <li>Foreign tables have TABLE_TYPE = "FOREIGN TABLE" (excluded from listing)
   * </ul>
   *
   * <p>This method overrides the parent to include regular tables and partition parent tables, but
   * excludes views and foreign tables from the table list.
   *
   * @param connection the database connection
   * @return ResultSet containing table metadata
   * @throws SQLException if a database access error occurs
   */
  @Override
  protected ResultSet getTables(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String catalogName = connection.getCatalog();
    String schemaName = connection.getSchema();
    // Include "TABLE" (regular tables and partition children),
    // and "PARTITIONED TABLE" (partition parent tables)
    // Exclude "VIEW" and "FOREIGN TABLE" to hide views and foreign tables from Gravitino
    return metaData.getTables(
        catalogName, schemaName, null, new String[] {"TABLE", "PARTITIONED TABLE"});
  }

  @Override
  protected ResultSet getTable(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    // Include TABLE and PARTITIONED TABLE types
    // Exclude VIEW and FOREIGN TABLE to hide views and foreign tables from Gravitino
    return metaData.getTables(
        database, schema, tableName, new String[] {"TABLE", "PARTITIONED TABLE"});
  }

  @Override
  protected ResultSet getColumns(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(database, schema, tableName, null);
  }

  /**
   * Get distribution information from Hologres system table hologres.hg_table_properties.
   *
   * <p>In Hologres, distribution_key is stored as a table property with the property_key
   * "distribution_key" and property_value as comma-separated column names (e.g., "col1,col2").
   *
   * <p>This method queries the system table and returns a HASH distribution with the specified
   * columns. Hologres only supports HASH distribution strategy.
   *
   * @param connection the database connection
   * @param databaseName the schema name
   * @param tableName the table name
   * @return the distribution info, or {@link Distributions#NONE} if no distribution_key is set
   * @throws SQLException if a database access error occurs
   */
  @Override
  protected Distribution getDistributionInfo(
      Connection connection, String databaseName, String tableName) throws SQLException {
    String schemaName = connection.getSchema();
    String distributionSql =
        "SELECT property_value "
            + "FROM hologres.hg_table_properties "
            + "WHERE table_namespace = ? AND table_name = ? AND property_key = 'distribution_key'";

    try (PreparedStatement statement = connection.prepareStatement(distributionSql)) {
      statement.setString(1, schemaName);
      statement.setString(2, tableName);

      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          String distributionKey = resultSet.getString("property_value");
          if (StringUtils.isNotEmpty(distributionKey)) {
            NamedReference[] columns =
                Arrays.stream(distributionKey.split(","))
                    .map(String::trim)
                    .filter(StringUtils::isNotEmpty)
                    .map(NamedReference::field)
                    .toArray(NamedReference[]::new);
            if (columns.length > 0) {
              return Distributions.hash(0, columns);
            }
          }
        }
      }
    }
    return Distributions.NONE;
  }

  /**
   * Validate the distribution for Hologres.
   *
   * <p>Hologres only supports HASH distribution strategy.
   *
   * @param distribution the distribution to validate
   */
  private void validateDistribution(Distribution distribution) {
    Preconditions.checkArgument(
        distribution.strategy() == Strategy.HASH,
        "Hologres only supports HASH distribution strategy, but got: %s",
        distribution.strategy());
    Preconditions.checkArgument(
        distribution.expressions().length > 0,
        "Hologres HASH distribution requires at least one distribution column");
  }

  @Override
  public Integer calculateDatetimePrecision(String typeName, int columnSize, int scale) {
    String upperTypeName = typeName.toUpperCase();
    switch (upperTypeName) {
      case "TIME":
      case "TIMETZ":
      case "TIMESTAMP":
      case "TIMESTAMPTZ":
        return Math.max(scale, 0);
      default:
        return null;
    }
  }

  /**
   * Get table partitioning information from PostgreSQL system tables.
   *
   * <p>Hologres (PostgreSQL-compatible) only supports LIST partitioning. This method queries
   * pg_partitioned_table and pg_attribute system tables to determine if a table is partitioned, and
   * if so, returns the partition column names as a LIST transform.
   *
   * <p>The SQL queries follow the same approach used by Holo Client's ConnectionUtil:
   *
   * <ol>
   *   <li>Query pg_partitioned_table to check if the table is partitioned and get partition column
   *       attribute numbers (partattrs)
   *   <li>Query pg_attribute to resolve attribute numbers to column names
   * </ol>
   *
   * @param connection the database connection
   * @param databaseName the schema name
   * @param tableName the table name
   * @return the partition transforms, or empty if the table is not partitioned
   * @throws SQLException if a database access error occurs
   */
  @Override
  protected Transform[] getTablePartitioning(
      Connection connection, String databaseName, String tableName) throws SQLException {

    // First, check if this is a logical partitioned table by querying table properties.
    // Logical partition tables in Hologres (V3.1+) have the property
    // "is_logical_partitioned_table" set to "true", and partition columns are stored in
    // the "logical_partition_columns" property.
    Transform[] logicalPartitioning = getLogicalPartitioning(connection, tableName);
    if (logicalPartitioning.length > 0) {
      return logicalPartitioning;
    }

    // Fall back to physical partition table check via pg_partitioned_table system table
    return getPhysicalPartitioning(connection, databaseName, tableName);
  }

  /**
   * Get logical partition information from Hologres table properties.
   *
   * <p>Hologres V3.1+ supports logical partition tables where the parent table is a physical table
   * and child partitions are logical concepts. A logical partition table is identified by the
   * property "is_logical_partitioned_table" = "true" in hologres.hg_table_properties, and its
   * partition columns are stored in the "logical_partition_columns" property.
   *
   * @param connection the database connection
   * @param tableName the table name
   * @return the partition transforms, or empty if the table is not a logical partition table
   * @throws SQLException if a database access error occurs
   */
  private Transform[] getLogicalPartitioning(Connection connection, String tableName)
      throws SQLException {
    String schemaName = connection.getSchema();
    String logicalPartitionSql =
        "SELECT property_key, property_value "
            + "FROM hologres.hg_table_properties "
            + "WHERE table_namespace = ? AND table_name = ? "
            + "AND property_key IN ('is_logical_partitioned_table', 'logical_partition_columns')";

    String isLogicalPartitioned = null;
    String logicalPartitionColumns = null;

    try (PreparedStatement statement = connection.prepareStatement(logicalPartitionSql)) {
      statement.setString(1, schemaName);
      statement.setString(2, tableName);

      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          String key = resultSet.getString("property_key");
          String value = resultSet.getString("property_value");
          if ("is_logical_partitioned_table".equals(key)) {
            isLogicalPartitioned = value;
          } else if ("logical_partition_columns".equals(key)) {
            logicalPartitionColumns = value;
          }
        }
      }
    }

    if (!"true".equalsIgnoreCase(isLogicalPartitioned)
        || StringUtils.isEmpty(logicalPartitionColumns)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    // Parse partition column names (comma-separated, e.g., "col1" or "col1,col2")
    String[][] fieldNames =
        Arrays.stream(logicalPartitionColumns.split(","))
            .map(String::trim)
            .filter(StringUtils::isNotEmpty)
            .map(col -> new String[] {col})
            .toArray(String[][]::new);

    if (fieldNames.length == 0) {
      return Transforms.EMPTY_TRANSFORM;
    }

    return new Transform[] {Transforms.list(fieldNames)};
  }

  /**
   * Get physical partition information from PostgreSQL system tables.
   *
   * <p>Hologres (PostgreSQL-compatible) only supports LIST partitioning for physical partitions.
   * This method queries pg_partitioned_table and pg_attribute system tables to determine if a table
   * is partitioned, and if so, returns the partition column names as a LIST transform.
   *
   * @param connection the database connection
   * @param databaseName the schema name
   * @param tableName the table name
   * @return the partition transforms, or empty if the table is not a physical partition table
   * @throws SQLException if a database access error occurs
   */
  private Transform[] getPhysicalPartitioning(
      Connection connection, String databaseName, String tableName) throws SQLException {

    // Query pg_partitioned_table to get partition strategy and column attribute numbers
    String partitionSql =
        "SELECT part.partstrat, part.partnatts, part.partattrs "
            + "FROM pg_catalog.pg_class c "
            + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            + "JOIN pg_catalog.pg_partitioned_table part ON c.oid = part.partrelid "
            + "WHERE n.nspname = ? AND c.relname = ? "
            + "LIMIT 1";

    String partStrategy = null;
    String partAttrs = null;

    try (PreparedStatement statement = connection.prepareStatement(partitionSql)) {
      statement.setString(1, databaseName);
      statement.setString(2, tableName);

      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          partStrategy = resultSet.getString("partstrat");
          partAttrs = resultSet.getString("partattrs");
        }
      }
    }

    // Not a partitioned table
    if (partStrategy == null || partAttrs == null) {
      return Transforms.EMPTY_TRANSFORM;
    }

    // Parse partition attribute numbers (e.g., "1" or "1 2")
    String[] attrNums = partAttrs.trim().split("\\s+");
    List<String[]> partitionColumnNames = new ArrayList<>();

    // Resolve attribute numbers to column names
    String attrSql =
        "SELECT attname FROM pg_catalog.pg_attribute "
            + "WHERE attrelid = (SELECT c.oid FROM pg_catalog.pg_class c "
            + "  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            + "  WHERE n.nspname = ? AND c.relname = ?) "
            + "AND attnum = ?";

    for (String attrNum : attrNums) {
      try (PreparedStatement statement = connection.prepareStatement(attrSql)) {
        statement.setString(1, databaseName);
        statement.setString(2, tableName);
        statement.setInt(3, Integer.parseInt(attrNum));

        try (ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            partitionColumnNames.add(new String[] {resultSet.getString("attname")});
          }
        }
      }
    }

    if (partitionColumnNames.isEmpty()) {
      return Transforms.EMPTY_TRANSFORM;
    }

    // Hologres only supports LIST partitioning (partstrat = 'l')
    // Return a LIST transform with the partition column names
    String[][] fieldNames = partitionColumnNames.toArray(new String[0][]);
    return new Transform[] {Transforms.list(fieldNames)};
  }

  /**
   * Get table properties from Hologres system table hologres.hg_table_properties.
   *
   * <p>This method queries the Hologres system table to retrieve table properties such as:
   *
   * <ul>
   *   <li>orientation: storage format (row/column/row,column)
   *   <li>clustering_key: clustering key columns
   *   <li>segment_key: event time column (segment key)
   *   <li>bitmap_columns: bitmap index columns
   *   <li>dictionary_encoding_columns: dictionary encoding columns
   *   <li>primary_key: primary key columns
   *   <li>time_to_live_in_seconds: TTL setting
   *   <li>table_group: table group name
   * </ul>
   *
   * @param connection the database connection
   * @param tableName the name of the table
   * @return a map of table properties
   * @throws SQLException if a database access error occurs
   */
  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    Map<String, String> properties = new HashMap<>();
    String schemaName = connection.getSchema();

    // Query table properties from hologres.hg_table_properties system table
    // The system table stores each property as a separate row with property_key and property_value
    String propertiesSql =
        "SELECT property_key, property_value "
            + "FROM hologres.hg_table_properties "
            + "WHERE table_namespace = ? AND table_name = ?";

    try (PreparedStatement statement = connection.prepareStatement(propertiesSql)) {
      statement.setString(1, schemaName);
      statement.setString(2, tableName);

      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          String propertyKey = resultSet.getString("property_key");
          String propertyValue = resultSet.getString("property_value");

          // Only include meaningful properties that users care about
          if (StringUtils.isNotEmpty(propertyValue) && isUserRelevantProperty(propertyKey)) {
            // Convert JDBC property keys to DDL-compatible keys
            // Hologres system table stores "binlog.level" and "binlog.ttl",
            // but CREATE TABLE WITH clause uses "binlog_level" and "binlog_ttl"
            String normalizedKey = convertFromJdbcPropertyKey(propertyKey);
            properties.put(normalizedKey, propertyValue);
          }
        }
      }
    }

    LOG.debug("Loaded table properties for {}.{}: {}", schemaName, tableName, properties);
    return properties;
  }

  /**
   * Convert JDBC property key to DDL-compatible property key.
   *
   * <p>Hologres system table {@code hologres.hg_table_properties} stores some property keys with
   * dots (e.g., "binlog.level", "binlog.ttl"), but the CREATE TABLE WITH clause uses underscores
   * (e.g., "binlog_level", "binlog_ttl"). This method converts from the JDBC format to the DDL
   * format so that properties can be round-tripped correctly.
   *
   * @param jdbcKey the property key from the JDBC query
   * @return the DDL-compatible property key
   */
  private String convertFromJdbcPropertyKey(String jdbcKey) {
    switch (jdbcKey) {
      case "binlog.level":
        return "binlog_level";
      case "binlog.ttl":
        return "binlog_ttl";
      default:
        return jdbcKey;
    }
  }

  /**
   * Check if a property key is relevant for users to see.
   *
   * <p>This filters out internal system properties and only returns properties that are meaningful
   * for users.
   *
   * @param propertyKey the property key to check
   * @return true if the property is relevant for users
   */
  private boolean isUserRelevantProperty(String propertyKey) {
    // List of properties that are meaningful for users
    switch (propertyKey) {
      case "orientation":
      case "clustering_key":
      case "segment_key": // event_time_column
      case "bitmap_columns":
      case "dictionary_encoding_columns":
      case "time_to_live_in_seconds":
      case "table_group":
      case "storage_format":
      case "binlog.level":
      case "binlog.ttl":
      case "is_logical_partitioned_table":
      case "partition_expiration_time":
      case "partition_keep_hot_window":
      case "partition_require_filter":
      case "partition_generate_binlog_window":
        return true;
      default:
        // Exclude internal properties like table_id, schema_version, etc.
        return false;
    }
  }
}
