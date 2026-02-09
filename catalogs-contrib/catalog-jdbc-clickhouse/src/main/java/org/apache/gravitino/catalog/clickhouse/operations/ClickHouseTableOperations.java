/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE_PROPERTY_ENTRY;
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
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.TableConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

public class ClickHouseTableOperations extends JdbcTableOperations {

  private static final String CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Clickhouse does not support nested column names.";

  private static final String QUERY_INDEXES_SQL =
      """
      SELECT NULL AS TABLE_CAT,
             system.tables.database AS TABLE_SCHEM,
             system.tables.name AS TABLE_NAME,
             trim(c.1) AS COLUMN_NAME,
             c.2 AS KEY_SEQ,
             'PRIMARY' AS PK_NAME
      FROM system.tables
      ARRAY JOIN arrayZip(splitByChar(',', primary_key), arrayEnumerate(splitByChar(',', primary_key))) as c
      WHERE system.tables.primary_key <> ''
        AND system.tables.database = '%s'
        AND system.tables.name = '%s'
      ORDER BY COLUMN_NAME
      """;

  @Override
  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName) {
    // cause clickhouse not impl getPrimaryKeys yet, ref:
    // https://github.com/ClickHouse/clickhouse-java/issues/1625
    String sql =
        QUERY_INDEXES_SQL.formatted(quoteIdentifier(databaseName), quoteIdentifier(tableName));
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      List<Index> indexes = new ArrayList<>();
      while (resultSet.next()) {
        String indexName = resultSet.getString("PK_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        indexes.add(
            Indexes.of(
                Index.IndexType.PRIMARY_KEY, indexName, new String[][] {{columnName}}, Map.of()));
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

    // These two are not yet supported in Gravitino now and will be supported in the future.
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in clickhouse");
    }
    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution), "ClickHouse does not support distribution");

    // First build the CREATE TABLE statement
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE %s (\n".formatted(quoteIdentifier(tableName)));

    // Add columns
    buildColumnsDefinition(columns, sqlBuilder);

    // Index definition, we only support primary index now, secondary index will be supported in
    // the future
    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append("\n)");

    // Extract engine from properties
    ClickHouseTablePropertiesMetadata.ENGINE engine = appendTableEngine(properties, sqlBuilder);

    appendOrderBy(sortOrders, sqlBuilder, engine);

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      String escapedComment = comment.replace("'", "''");
      sqlBuilder.append(" COMMENT '%s'".formatted(escapedComment));
    }

    // Add setting clause if specified, clickhouse only supports predefine settings
    appendTableProperties(properties, sqlBuilder);

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  private static void appendTableProperties(
      Map<String, String> properties, StringBuilder sqlBuilder) {
    if (MapUtils.isEmpty(properties)) {
      return;
    }

    String settings =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(TableConstants.SETTINGS_PREFIX))
            .map(
                entry ->
                    entry.getKey().substring(TableConstants.SETTINGS_PREFIX.length())
                        + " = "
                        + entry.getValue())
            .collect(Collectors.joining(",\n ", " \n SETTINGS ", ""));
    sqlBuilder.append(settings);
  }

  private static void appendOrderBy(
      SortOrder[] sortOrders,
      StringBuilder sqlBuilder,
      ClickHouseTablePropertiesMetadata.ENGINE engine) {
    // ClickHouse requires ORDER BY clause for some engines, and currently only mergeTree family
    // requires ORDER BY clause.
    boolean requireOrderBy = engine.isRequireOrderBy();
    if (!requireOrderBy) {
      if (ArrayUtils.isNotEmpty(sortOrders)) {
        throw new UnsupportedOperationException(
            "ORDER BY clause is not supported for engine: " + engine.getValue());
      }

      // No need to add order by clause
      return;
    }

    if (ArrayUtils.isEmpty(sortOrders)) {
      throw new IllegalArgumentException(
          "ORDER BY clause is required for engine: " + engine.getValue());
    }

    if (sortOrders.length > 1) {
      throw new UnsupportedOperationException(
          "Currently ClickHouse does not support sortOrders with more than 1 element");
    }

    NullOrdering nullOrdering = sortOrders[0].nullOrdering();
    SortDirection sortDirection = sortOrders[0].direction();
    if (nullOrdering != null && sortDirection != null) {
      // ClickHouse does not support NULLS FIRST/LAST now.
      LOG.warn(
          "ClickHouse currently does not support nullOrdering: {}, and will ignore it",
          nullOrdering);
    }

    sqlBuilder.append("\n ORDER BY `%s`\n".formatted(sortOrders[0].expression()));
  }

  private ClickHouseTablePropertiesMetadata.ENGINE appendTableEngine(
      Map<String, String> properties, StringBuilder sqlBuilder) {
    ClickHouseTablePropertiesMetadata.ENGINE engine = ENGINE_PROPERTY_ENTRY.getDefaultValue();
    if (MapUtils.isNotEmpty(properties)) {
      String userSetEngine = properties.remove(CLICKHOUSE_ENGINE_KEY);
      if (StringUtils.isNotEmpty(userSetEngine)) {
        engine = ClickHouseTablePropertiesMetadata.ENGINE.fromString(userSetEngine);
      }
    }
    sqlBuilder.append("\n ENGINE = %s".formatted(engine.getValue()));
    return engine;
  }

  private void buildColumnsDefinition(JdbcColumn[] columns, StringBuilder sqlBuilder) {
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("  %s".formatted(quoteIdentifier(column.name())));

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",\n");
      }
    }
  }

  /**
   * ClickHouse only supports primary key now, and some secondary index will be supported in future
   *
   * <p>This method will not check the validity of the indexes. For clickhouse, the primary key must
   * be a subset of the order by columns. We will leave the underlying clickhouse to validate it.
   */
  private void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    if (ArrayUtils.isEmpty(indexes)) {
      return;
    }

    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          if (null != index.name()
              && !StringUtils.equalsIgnoreCase(index.name(), Indexes.DEFAULT_PRIMARY_KEY_NAME)) {
            LOG.warn(
                "Primary key name must be PRIMARY in ClickHouse, the name {} will be ignored.",
                index.name());
          }
          // fieldStr already quoted in getIndexFieldStr
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
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
  public void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException {
    LOG.info("Attempting to alter table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      String sql = generateAlterTableSql(databaseName, tableName, changes);
      if (StringUtils.isEmpty(sql)) {
        LOG.info("No changes to alter table {} from database {}", tableName, databaseName);
        return;
      }
      JdbcConnectorUtils.executeUpdate(connection, sql);
      LOG.info("Alter table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
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
    return metaData.getTables(catalogName, schemaName, null, new String[] {"TABLE"});
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

    for (TableChange change : changes) {
      if (change instanceof TableChange.UpdateComment) {
        updateComment = (TableChange.UpdateComment) change;

      } else if (change instanceof TableChange.SetProperty setProperty) {
        // The set attribute needs to be added at the end.
        setProperties.add(setProperty);

      } else if (change instanceof TableChange.RemoveProperty) {
        // Clickhouse does not support deleting table attributes, it can be replaced by Set Property
        throw new UnsupportedOperationException(
            "Remove property for ClickHouse is not supported yet");

      } else if (change instanceof TableChange.AddColumn addColumn) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addColumnFieldDefinition(addColumn));

      } else if (change instanceof TableChange.RenameColumn renameColumn) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(renameColumnFieldDefinition(renameColumn));

      } else if (change instanceof TableChange.UpdateColumnDefaultValue updateColumnDefaultValue) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(
            updateColumnDefaultValueFieldDefinition(updateColumnDefaultValue, lazyLoadTable));

      } else if (change instanceof TableChange.UpdateColumnType updateColumnType) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));

      } else if (change instanceof TableChange.UpdateColumnComment updateColumnComment) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnCommentFieldDefinition(updateColumnComment, lazyLoadTable));

      } else if (change instanceof TableChange.UpdateColumnPosition updateColumnPosition) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnPositionFieldDefinition(updateColumnPosition, lazyLoadTable));

      } else if (change instanceof TableChange.DeleteColumn deleteColumn) {
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
        // Auto increment functionality was added in ClickHouse 25.1. Since this PR is based on
        // 23.x, we throw unsupported operation here.
        throw new UnsupportedOperationException(
            "ClickHouse auto increment is not supported in this version.");
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type: " + change.getClass().getName());
      }
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
      String escapedComment = newComment.replace("'", "''");
      alterSql.add(" MODIFY COMMENT '%s'".formatted(escapedComment));
    }

    if (!setProperties.isEmpty()) {
      alterSql.add(generateAlterTableProperties(setProperties));
    }

    // Remove all empty SQL statements
    List<String> nonEmptySQLs =
        alterSql.stream().filter(StringUtils::isNotEmpty).collect(Collectors.toList());
    if (CollectionUtils.isEmpty(nonEmptySQLs)) {
      return "";
    }

    // Return the generated SQL statement
    String result =
        "ALTER TABLE %s \n%s;"
            .formatted(quoteIdentifier(tableName), String.join(",\n", nonEmptySQLs));
    LOG.info("Generated alter table:{} sql: {}", databaseName + "." + tableName, result);
    return result;
  }

  @VisibleForTesting
  private String deleteIndexDefinition(
      JdbcTable lazyLoadTable, TableChange.DeleteIndex deleteIndex) {
    boolean indexExists =
        Arrays.stream(lazyLoadTable.index())
            .anyMatch(index -> index.name().equals(deleteIndex.getName()));

    // Index does not exist
    if (!indexExists) {
      // If ifExists is true, return empty string to skip the operation
      if (deleteIndex.isIfExists()) {
        return "";
      } else {
        throw new IllegalArgumentException(
            "Index '%s' does not exist".formatted(deleteIndex.getName()));
      }
    }

    return "DROP INDEX %s ".formatted(quoteIdentifier(deleteIndex.getName()));
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

    return "%s %s %s"
        .formatted(
            MODIFY_COLUMN,
            quoteIdentifier(col),
            appendColumnDefinition(updateColumn, new StringBuilder()));
  }

  private String generateAlterTableProperties(List<TableChange.SetProperty> setProperties) {
    if (CollectionUtils.isNotEmpty(setProperties)) {
      throw new UnsupportedOperationException(
          "Alter table properties in ClickHouse is not supported");
    }

    return "";
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

    return "%s %s %s"
        .formatted(
            MODIFY_COLUMN,
            quoteIdentifier(col),
            appendColumnDefinition(updateColumn, new StringBuilder()));
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    String dataType = typeConverter.fromGravitino(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = addColumn.fieldName()[0];
    StringBuilder columnDefinition = new StringBuilder();
    //  [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
    if (addColumn.isNullable()) {
      columnDefinition.append(
          "ADD COLUMN %s Nullable(%s) ".formatted(quoteIdentifier(col), dataType));
    } else {
      columnDefinition.append("ADD COLUMN %s %s ".formatted(quoteIdentifier(col), dataType));
    }

    if (addColumn.isAutoIncrement()) {
      throw new UnsupportedOperationException(
          "ClickHouse does not support adding auto increment column");
    }

    // Append default value if available
    if (!Column.DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      columnDefinition.append(
          "DEFAULT %s "
              .formatted(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue())));
    }

    // Append comment if available after default value
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      String escapedComment = StringUtils.replace(addColumn.getComment(), "'", "''");
      columnDefinition.append(" COMMENT '%s' ".formatted(escapedComment));
    }

    // Append position if available
    if (addColumn.getPosition() instanceof TableChange.First) {
      columnDefinition.append(" FIRST ");
    } else if (addColumn.getPosition() instanceof TableChange.After afterPosition) {
      columnDefinition.append(" AFTER %s ".formatted(quoteIdentifier(afterPosition.getColumn())));
    }

    return columnDefinition.toString();
  }

  private String renameColumnFieldDefinition(TableChange.RenameColumn renameColumn) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String oldColumnName = renameColumn.fieldName()[0];
    String newColumnName = renameColumn.getNewName();

    return "RENAME COLUMN %s TO %s"
        .formatted(quoteIdentifier(oldColumnName), quoteIdentifier(newColumnName));
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = updateColumnPosition.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append(" %s %s ".formatted(MODIFY_COLUMN, quoteIdentifier(col)));
    appendColumnDefinition(column, columnDefinition);

    if (updateColumnPosition.getPosition() instanceof TableChange.First) {
      columnDefinition.append(" FIRST ");
    } else if (updateColumnPosition.getPosition() instanceof TableChange.After afterPosition) {
      columnDefinition.append(
          " %s %s ".formatted(AFTER, quoteIdentifier(afterPosition.getColumn())));
    } else {
      Arrays.stream(jdbcTable.columns())
          .reduce((column1, column2) -> column2)
          .map(Column::name)
          .ifPresent(s -> columnDefinition.append(" %s %s ".formatted(AFTER, quoteIdentifier(s))));
    }
    return columnDefinition.toString();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable jdbcTable) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = deleteColumn.fieldName()[0];
    boolean colExists = columnExists(jdbcTable, col);
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column '%s' does not exist.".formatted(col));
      }
    }

    return "DROP COLUMN %s".formatted(quoteIdentifier(col));
  }

  private String updateColumnDefaultValueFieldDefinition(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, JdbcTable jdbcTable) {
    if (updateColumnDefaultValue.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = updateColumnDefaultValue.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder = new StringBuilder(MODIFY_COLUMN + quoteIdentifier(col));
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
    StringBuilder sqlBuilder =
        new StringBuilder("%s %s ".formatted(MODIFY_COLUMN, quoteIdentifier(col)));

    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName(col)
            .withType(updateColumnType.getNewDataType())
            .withComment(column.comment())
            .withDefaultValue(column.defaultValue())
            .withNullable(column.nullable())
            .withAutoIncrement(column.autoIncrement())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private StringBuilder appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add Nullable data type
    String dataType = typeConverter.fromGravitino(column.dataType());
    if (column.nullable()) {
      sqlBuilder.append(" Nullable(%s) ".formatted(dataType));
    } else {
      sqlBuilder.append(" %s ".formatted(dataType));
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder.append(
          " DEFAULT %s "
              .formatted(columnDefaultValueConverter.fromGravitino(column.defaultValue())));
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      String escapedComment = StringUtils.replace(column.comment(), "'", "''");
      sqlBuilder.append("COMMENT '%s' ".formatted(escapedComment));
    }

    return sqlBuilder;
  }
}
