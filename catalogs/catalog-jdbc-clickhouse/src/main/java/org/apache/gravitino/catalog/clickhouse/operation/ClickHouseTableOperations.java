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
package org.apache.gravitino.catalog.clickhouse.operation;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;

/** Table operations for ClickHouse. */
public class ClickHouseTableOperations extends JdbcTableOperations {

  private static final String BACK_QUOTE = "`";
  private static final String CLICKHOUSE_AUTO_INCREMENT = "AUTO_INCREMENT";
  private static final String CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Clickhouse does not support nested column names.";

  /**
   * ClickHouse does not support some multiple changes in one statement, So rewrite this method, one
   * by one to apply TableChange to the table.
   *
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @param changes The changes to apply to the table.
   */
  @Override
  public void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException {
    LOG.info("Attempting to alter table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      for (TableChange change : changes) {
        String sql = generateAlterTableSql(databaseName, tableName, change);
        if (StringUtils.isEmpty(sql)) {
          LOG.info("No changes to alter table {} from database {}", tableName, databaseName);
          return;
        }
        JdbcConnectorUtils.executeUpdate(connection, sql);
      }
      LOG.info("Alter table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName)
      throws SQLException {
    // cause clickhouse not impl getPrimaryKeys yet, ref:
    // https://github.com/ClickHouse/clickhouse-java/issues/1625
    String sql =
        "SELECT NULL AS TABLE_CAT, "
            + "system.tables.database AS TABLE_SCHEM, "
            + "system.tables.name AS TABLE_NAME, "
            + "trim(c.1) AS COLUMN_NAME, "
            + "c.2 AS KEY_SEQ, "
            + "'PRIMARY' AS PK_NAME "
            + "FROM system.tables "
            + "ARRAY JOIN arrayZip(splitByChar(',', primary_key), arrayEnumerate(splitByChar(',', primary_key))) as c "
            + "WHERE system.tables.primary_key <> '' "
            + "AND system.tables.database = '"
            + databaseName
            + "' "
            + "AND system.tables.name = '"
            + tableName
            + "' "
            + "ORDER BY COLUMN_NAME";

    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      List<Index> indexes = new ArrayList<>();
      while (resultSet.next()) {
        String indexName = resultSet.getString("PK_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        indexes.add(
            Indexes.of(Index.IndexType.PRIMARY_KEY, indexName, new String[][] {{columnName}}));
      }
      return indexes;
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
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
    throw new UnsupportedOperationException(
        "generateCreateTableSql with out sortOrders in clickhouse is not supported");
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes,
      SortOrder[] sortOrders) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in clickhouse");
    }

    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution), "ClickHouse does not support distribution");

    validateIncrementCol(columns, indexes);
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(BACK_QUOTE)
        .append(tableName)
        .append(BACK_QUOTE)
        .append(" (\n");

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder
          .append(SPACE)
          .append(SPACE)
          .append(BACK_QUOTE)
          .append(column.name())
          .append(BACK_QUOTE);

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",\n");
      }
    }

    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append("\n)");

    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      sqlBuilder.append(
          properties.entrySet().stream()
              .map(entry -> String.format("%s = %s", entry.getKey(), entry.getValue()))
              .collect(Collectors.joining(",\n", "\n", "")));
    }

    if (ArrayUtils.isNotEmpty(sortOrders)) {
      if (sortOrders.length > 1) {
        throw new UnsupportedOperationException(
            "Currently we do not support sortOrders's length > 1");
      } else if (sortOrders[0].nullOrdering() != null || sortOrders[0].direction() != null) {
        // If no value is set earlier, some default values will be set.
        // It is difficult to determine whether the user has set a value.
        LOG.warn(
            "clickhouse currently do not support nullOrdering: {} and direction: {} of sortOrders,and will ignore these",
            sortOrders[0].nullOrdering(),
            sortOrders[0].direction());
      }
      sqlBuilder.append(
          " \n ORDER BY " + BACK_QUOTE + sortOrders[0].expression() + BACK_QUOTE + " \n");
    }

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder.append(" COMMENT '").append(comment).append("'");
    }

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  public static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    if (indexes == null) {
      return;
    }

    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          if (null != index.name()
              && !StringUtils.equalsIgnoreCase(
                  index.name(), Indexes.DEFAULT_CLICKHOUSE_PRIMARY_KEY_NAME)) {
            LOG.warn(
                "Primary key name must be PRIMARY in ClickHouse, the name {} will be ignored.",
                index.name());
          }
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          throw new IllegalArgumentException(
              "Gravitino clickHouse doesn't support index : " + index.type());
        default:
          throw new IllegalArgumentException(
              "Gravitino Clickhouse doesn't support index : " + index.type());
      }
    }
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT"));
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement("select * from system.tables where name = ? ")) {
      statement.setString(1, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          String name = resultSet.getString("name");
          if (Objects.equals(name, tableName)) {
            return Collections.unmodifiableMap(
                new HashMap<String, String>() {
                  {
                    put(COMMENT, resultSet.getString(COMMENT));
                    put(CLICKHOUSE_ENGINE_KEY, resultSet.getString(CLICKHOUSE_ENGINE_KEY));
                  }
                });
          }
        }

        throw new NoSuchTableException(
            "Table %s does not exist in %s.", tableName, connection.getCatalog());
      }
    }
  }

  protected ResultSet getTables(Connection connection) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String catalogName = connection.getCatalog();
    String schemaName = connection.getSchema();
    // CK tables include : DICTIONARY", "LOG TABLE", "MEMORY TABLE",
    // "REMOTE TABLE", "TABLE", "VIEW", "SYSTEM TABLE", "TEMPORARY TABLE
    return metaData.getTables(catalogName, schemaName, null, null);
  }

  @Override
  protected void correctJdbcTableFields(
      Connection connection, String databaseName, String tableName, JdbcTable.Builder tableBuilder)
      throws SQLException {
    if (StringUtils.isEmpty(tableBuilder.comment())) {
      // In Clickhouse version 5.7, the comment field value cannot be obtained in the driver API.
      LOG.warn("Not found comment in clickhouse driver api. Will try to get comment from sql");
      tableBuilder.withComment(
          tableBuilder.properties().getOrDefault(COMMENT, tableBuilder.comment()));
    }
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "ClickHouse does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    // Not all operations require the original table information, so lazy loading is used here
    JdbcTable lazyLoadTable = null;
    TableChange.UpdateComment updateComment = null;
    List<TableChange.SetProperty> setProperties = new ArrayList<>();
    List<String> alterSql = new ArrayList<>();
    for (int i = 0; i < changes.length; i++) {
      TableChange change = changes[i];
      if (change instanceof TableChange.UpdateComment) {
        updateComment = (TableChange.UpdateComment) change;
      } else if (change instanceof TableChange.SetProperty) {
        // The set attribute needs to be added at the end.
        setProperties.add(((TableChange.SetProperty) change));
      } else if (change instanceof TableChange.RemoveProperty) {
        // clickhouse does not support deleting table attributes, it can be replaced by Set Property
        throw new IllegalArgumentException("Remove property is not supported yet");
      } else if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addColumnFieldDefinition(addColumn));
      } else if (change instanceof TableChange.RenameColumn) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        alterSql.add(renameColumnFieldDefinition(renameColumn));
      } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
            (TableChange.UpdateColumnDefaultValue) change;
        alterSql.add(
            updateColumnDefaultValueFieldDefinition(updateColumnDefaultValue, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnType) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnComment) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        TableChange.UpdateColumnComment updateColumnComment =
            (TableChange.UpdateColumnComment) change;
        alterSql.add(updateColumnCommentFieldDefinition(updateColumnComment, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        TableChange.UpdateColumnPosition updateColumnPosition =
            (TableChange.UpdateColumnPosition) change;
        alterSql.add(updateColumnPositionFieldDefinition(updateColumnPosition, lazyLoadTable));
      } else if (change instanceof TableChange.DeleteColumn) {
        TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        String deleteColSql = deleteColumnFieldDefinition(deleteColumn, lazyLoadTable);
        if (StringUtils.isNotEmpty(deleteColSql)) {
          alterSql.add(deleteColSql);
        }
      } else if (change instanceof TableChange.UpdateColumnNullability) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(
            updateColumnNullabilityDefinition(
                (TableChange.UpdateColumnNullability) change, lazyLoadTable));
      } else if (change instanceof TableChange.DeleteIndex) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(deleteIndexDefinition(lazyLoadTable, (TableChange.DeleteIndex) change));
      } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(
            updateColumnAutoIncrementDefinition(
                lazyLoadTable, (TableChange.UpdateColumnAutoIncrement) change));
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type: " + change.getClass().getName());
      }
    }
    if (!setProperties.isEmpty()) {
      alterSql.add(generateAlterTableProperties(setProperties));
    }

    // Last modified comment
    if (null != updateComment) {
      String newComment = updateComment.getNewComment();
      if (null == StringIdentifier.fromComment(newComment)) {
        // Detect and add Gravitino id.
        JdbcTable jdbcTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
      alterSql.add(" MODIFY COMMENT '" + newComment + "'");
    }

    if (!setProperties.isEmpty()) {
      alterSql.add(generateAlterTableProperties(setProperties));
    }

    if (CollectionUtils.isEmpty(alterSql)) {
      return "";
    }
    // Return the generated SQL statement
    String result = "ALTER TABLE `" + tableName + "`\n" + String.join(",\n", alterSql) + ";";
    LOG.info("Generated alter table:{} sql: {}", databaseName + "." + tableName, result);
    return result;
  }

  private String updateColumnAutoIncrementDefinition(
      JdbcTable table, TableChange.UpdateColumnAutoIncrement change) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException("Nested column names are not supported");
    }
    String col = change.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(table, col);
    if (change.isAutoIncrement()) {
      Preconditions.checkArgument(
          Types.allowAutoIncrement(column.dataType()),
          "Auto increment is not allowed, type: " + column.dataType());
    }
    JdbcColumn updateColumn =
        JdbcColumn.builder()
            .withName(col)
            .withDefaultValue(column.defaultValue())
            .withNullable(column.nullable())
            .withType(column.dataType())
            .withComment(column.comment())
            .withAutoIncrement(change.isAutoIncrement())
            .build();
    return MODIFY_COLUMN
        + BACK_QUOTE
        + col
        + BACK_QUOTE
        + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  @VisibleForTesting
  static String deleteIndexDefinition(
      JdbcTable lazyLoadTable, TableChange.DeleteIndex deleteIndex) {
    if (deleteIndex.isIfExists()) {
      if (Arrays.stream(lazyLoadTable.index())
          .anyMatch(index -> index.name().equals(deleteIndex.getName()))) {
        throw new IllegalArgumentException("Index does not exist");
      }
    }
    return "DROP INDEX " + BACK_QUOTE + deleteIndex.getName() + BACK_QUOTE;
  }

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability change, JdbcTable table) {
    validateUpdateColumnNullable(change, table);
    String col = change.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(table, col);
    JdbcColumn updateColumn =
        JdbcColumn.builder()
            .withName(col)
            .withDefaultValue(column.defaultValue())
            .withNullable(change.nullable())
            .withType(column.dataType())
            .withComment(column.comment())
            .withAutoIncrement(column.autoIncrement())
            .build();
    return MODIFY_COLUMN
        + BACK_QUOTE
        + col
        + BACK_QUOTE
        + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  private String generateAlterTableProperties(List<TableChange.SetProperty> setProperties) {
    if (CollectionUtils.isNotEmpty(setProperties)) {
      throw new UnsupportedOperationException("alter table properties in ck is not supported");
    }

    return "";
    //    return setProperties.stream()
    //        .map(
    //            setProperty ->
    //                String.format("%s = %s", setProperty.getProperty(), setProperty.getValue()))
    //        .collect(Collectors.joining(",\n"));
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, JdbcTable jdbcTable) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnComment.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    JdbcColumn updateColumn =
        JdbcColumn.builder()
            .withName(col)
            .withDefaultValue(column.defaultValue())
            .withNullable(column.nullable())
            .withType(column.dataType())
            .withComment(newComment)
            .withAutoIncrement(column.autoIncrement())
            .build();
    return MODIFY_COLUMN
        + BACK_QUOTE
        + col
        + BACK_QUOTE
        + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    String dataType = typeConverter.fromGravitino(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = addColumn.fieldName()[0];

    StringBuilder columnDefinition = new StringBuilder();
    //  [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
    if (!addColumn.isNullable()) {
      columnDefinition
          .append("ADD COLUMN ")
          .append(BACK_QUOTE)
          .append(col)
          .append(BACK_QUOTE)
          .append(SPACE)
          .append(dataType)
          .append(SPACE);
    } else {
      columnDefinition
          .append("ADD COLUMN ")
          .append(BACK_QUOTE)
          .append(col)
          .append(BACK_QUOTE)
          .append(SPACE)
          .append("Nullable(")
          .append(dataType)
          .append(")")
          .append(SPACE);
    }

    if (addColumn.isAutoIncrement()) {
      Preconditions.checkArgument(
          Types.allowAutoIncrement(addColumn.getDataType()),
          "Auto increment is not allowed, type: " + addColumn.getDataType());
      columnDefinition.append(CLICKHOUSE_AUTO_INCREMENT).append(SPACE);
    }

    // Append default value if available
    if (!Column.DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      columnDefinition
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue()))
          .append(SPACE);
    }

    // Append comment if available after default value
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      columnDefinition.append("COMMENT '").append(addColumn.getComment()).append("' ");
    }

    // Append position if available
    if (addColumn.getPosition() instanceof TableChange.First) {
      columnDefinition.append("FIRST");
    } else if (addColumn.getPosition() instanceof TableChange.After) {
      TableChange.After afterPosition = (TableChange.After) addColumn.getPosition();
      columnDefinition
          .append(AFTER)
          .append(BACK_QUOTE)
          .append(afterPosition.getColumn())
          .append(BACK_QUOTE);
    } else if (addColumn.getPosition() instanceof TableChange.Default) {
      // do nothing, follow the default behavior of clickhouse
    } else {
      throw new IllegalArgumentException("Invalid column position.");
    }

    return columnDefinition.toString();
  }

  private String renameColumnFieldDefinition(TableChange.RenameColumn renameColumn) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String oldColumnName = renameColumn.fieldName()[0];
    String newColumnName = renameColumn.getNewName();
    StringBuilder sqlBuilder =
        new StringBuilder(
            "RENAME COLUMN "
                + BACK_QUOTE
                + oldColumnName
                + BACK_QUOTE
                + SPACE
                + "TO"
                + SPACE
                + BACK_QUOTE
                + newColumnName
                + BACK_QUOTE);
    return sqlBuilder.toString();
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnPosition.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append(MODIFY_COLUMN).append(quote(col));
    appendColumnDefinition(column, columnDefinition);
    if (updateColumnPosition.getPosition() instanceof TableChange.First) {
      columnDefinition.append("FIRST");
    } else if (updateColumnPosition.getPosition() instanceof TableChange.After) {
      TableChange.After afterPosition = (TableChange.After) updateColumnPosition.getPosition();
      columnDefinition.append(AFTER).append(afterPosition.getColumn());
    } else {
      Arrays.stream(jdbcTable.columns())
          .reduce((column1, column2) -> column2)
          .map(Column::name)
          .ifPresent(s -> columnDefinition.append(AFTER).append(s));
    }
    return columnDefinition.toString();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable jdbcTable) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = deleteColumn.fieldName()[0];
    boolean colExists = true;
    try {
      getJdbcColumnFromTable(jdbcTable, col);
    } catch (NoSuchColumnException noSuchColumnException) {
      colExists = false;
    }
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column does not exist: " + col);
      }
    }
    return "DROP COLUMN " + BACK_QUOTE + col + BACK_QUOTE;
  }

  private String updateColumnDefaultValueFieldDefinition(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, JdbcTable jdbcTable) {
    if (updateColumnDefaultValue.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnDefaultValue.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder = new StringBuilder(MODIFY_COLUMN + quote(col));
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName(col)
            .withType(column.dataType())
            .withNullable(column.nullable())
            .withComment(column.comment())
            .withDefaultValue(updateColumnDefaultValue.getNewDefaultValue())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder = new StringBuilder(MODIFY_COLUMN + quote(col));
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName(col)
            .withType(updateColumnType.getNewDataType())
            .withComment(column.comment())
            .withDefaultValue(DEFAULT_VALUE_NOT_SET)
            .withNullable(column.nullable())
            .withAutoIncrement(column.autoIncrement())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private StringBuilder appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add Nullable data type
    String dataType = typeConverter.fromGravitino(column.dataType());
    if (column.nullable()) {
      sqlBuilder.append(SPACE).append("Nullable(").append(dataType).append(")").append(SPACE);
    } else {
      sqlBuilder.append(SPACE).append(dataType).append(SPACE);
    }

    // ck no support alter table with set nullable

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }

    // Add column auto_increment if specified
    //    if (column.autoIncrement()) {
    //      sqlBuilder.append(CLICKHOUSE_AUTO_INCREMENT).append(" ");
    //    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
  }

  private static String quote(String name) {
    return BACK_QUOTE + name + BACK_QUOTE;
  }
}
