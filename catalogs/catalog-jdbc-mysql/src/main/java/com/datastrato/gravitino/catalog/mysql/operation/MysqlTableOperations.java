/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.operation;

import static com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata.MYSQL_AUTO_INCREMENT_OFFSET_KEY;
import static com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata.MYSQL_ENGINE_KEY;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for MySQL. */
public class MysqlTableOperations extends JdbcTableOperations {

  public static final String BACK_QUOTE = "`";
  public static final String MYSQL_AUTO_INCREMENT = "AUTO_INCREMENT";
  private static final String MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Mysql does not support nested column names.";

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(databaseName)) {
      try (Statement statement = connection.createStatement()) {
        String showTablesQuery = "SHOW TABLES";
        ResultSet resultSet = statement.executeQuery(showTablesQuery);
        List<String> names = new ArrayList<>();
        while (resultSet.next()) {
          String tableName = resultSet.getString(1);
          names.add(tableName);
        }
        LOG.info(
            "Finished listing tables size {} for database name {} ", names.size(), databaseName);
        return names;
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException("Currently we do not support Partitioning in mysql");
    }
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

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder.append(" COMMENT='").append(comment).append("'");
    }

    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      sqlBuilder.append(
          properties.entrySet().stream()
              .map(entry -> String.format("%s = %s", entry.getKey(), entry.getValue()))
              .collect(Collectors.joining(",\n", "\n", "")));
    }

    // Return the generated SQL statement
    String result = sqlBuilder.append(";").toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  /**
   * The auto-increment column will be verified. There can only be one auto-increment column and it
   * must be the primary key or unique index.
   *
   * @param columns jdbc column
   * @param indexes table indexes
   */
  private static void validateIncrementCol(JdbcColumn[] columns, Index[] indexes) {
    // Check auto increment column
    List<JdbcColumn> autoIncrementCols =
        Arrays.stream(columns).filter(Column::autoIncrement).collect(Collectors.toList());
    String autoIncrementColsStr =
        autoIncrementCols.stream().map(JdbcColumn::name).collect(Collectors.joining(",", "[", "]"));
    Preconditions.checkArgument(
        autoIncrementCols.size() <= 1,
        "Only one column can be auto-incremented. There are multiple auto-increment columns in your table: "
            + autoIncrementColsStr);
    if (!autoIncrementCols.isEmpty()) {
      Optional<Index> existAutoIncrementColIndexOptional =
          Arrays.stream(indexes)
              .filter(
                  index ->
                      Arrays.stream(index.fieldNames())
                          .flatMap(Arrays::stream)
                          .anyMatch(
                              s ->
                                  StringUtils.equalsIgnoreCase(autoIncrementCols.get(0).name(), s)))
              .filter(
                  index ->
                      index.type() == Index.IndexType.PRIMARY_KEY
                          || index.type() == Index.IndexType.UNIQUE_KEY)
              .findAny();
      Preconditions.checkArgument(
          existAutoIncrementColIndexOptional.isPresent(),
          "Incorrect table definition; there can be only one auto column and it must be defined as a key");
    }
  }

  public static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          if (null != index.name()
              && !StringUtils.equalsIgnoreCase(
                  index.name(), Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME)) {
            throw new IllegalArgumentException("Primary key name must be PRIMARY in MySQL");
          }
          sqlBuilder.append("CONSTRAINT ").append("PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          sqlBuilder.append("CONSTRAINT ");
          if (null != index.name()) {
            sqlBuilder.append(BACK_QUOTE).append(index.name()).append(BACK_QUOTE);
          }
          sqlBuilder.append(" UNIQUE (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException("MySQL doesn't support index : " + index.type());
      }
    }
  }

  private static String getIndexFieldStr(String[][] fieldNames) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in MySQL");
              }
              return BACK_QUOTE + colNames[0] + BACK_QUOTE;
            })
        .collect(Collectors.joining(", "));
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT"));
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    // MySQL in CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci is case-insensitive, when the name is
    // hello, the result can be 'HELLO', 'Hello', 'hello' and so on.
    try (PreparedStatement statement = connection.prepareStatement("SHOW TABLE STATUS LIKE ?")) {
      statement.setString(1, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          String name = resultSet.getString("NAME");
          if (Objects.equals(name, tableName)) {
            return Collections.unmodifiableMap(
                new HashMap<String, String>() {
                  {
                    put(COMMENT, resultSet.getString(COMMENT));
                    put(MYSQL_ENGINE_KEY, resultSet.getString(MYSQL_ENGINE_KEY));
                    String autoIncrement = resultSet.getString(MYSQL_AUTO_INCREMENT_OFFSET_KEY);
                    if (StringUtils.isNotEmpty(autoIncrement)) {
                      put(MYSQL_AUTO_INCREMENT_OFFSET_KEY, autoIncrement);
                    }
                  }
                });
          }
        }

        throw new NoSuchTableException(
            "Table %s does not exist in %s.", tableName, connection.getCatalog());
      }
    }
  }

  @Override
  protected void correctJdbcTableFields(
      Connection connection, String tableName, JdbcTable.Builder tableBuilder) throws SQLException {
    if (StringUtils.isEmpty(tableBuilder.comment())) {
      // In Mysql version 5.7, the comment field value cannot be obtained in the driver API.
      LOG.warn("Not found comment in mysql driver api. Will try to get comment from sql");
      tableBuilder.withComment(
          tableBuilder.properties().getOrDefault(COMMENT, tableBuilder.comment()));
    }
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return String.format("RENAME TABLE `%s` TO `%s`", oldTableName, newTableName);
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return "DROP TABLE " + BACK_QUOTE + tableName + BACK_QUOTE;
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "MySQL does not support purge table in Gravitino, please use drop table");
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
        // mysql does not support deleting table attributes, it can be replaced by Set Property
        throw new IllegalArgumentException("Remove property is not supported yet");
      } else if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addColumnFieldDefinition(addColumn));
      } else if (change instanceof TableChange.RenameColumn) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        alterSql.add(renameColumnFieldDefinition(renameColumn, lazyLoadTable));
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
      } else if (change instanceof TableChange.AddIndex) {
        alterSql.add(addIndexDefinition((TableChange.AddIndex) change));
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
      alterSql.add(generateTableProperties(setProperties));
    }

    // Last modified comment
    if (null != updateComment) {
      String newComment = updateComment.getNewComment();
      if (null == StringIdentifier.fromComment(newComment)) {
        // Detect and add gravitino id.
        JdbcTable jdbcTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
      alterSql.add("COMMENT '" + newComment + "'");
    }

    if (!setProperties.isEmpty()) {
      alterSql.add(generateTableProperties(setProperties));
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
        new JdbcColumn.Builder()
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
        new JdbcColumn.Builder()
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

  @VisibleForTesting
  static String addIndexDefinition(TableChange.AddIndex addIndex) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("ADD ");
    switch (addIndex.getType()) {
      case PRIMARY_KEY:
        if (null != addIndex.getName()
            && !StringUtils.equalsIgnoreCase(
                addIndex.getName(), Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME)) {
          throw new IllegalArgumentException("Primary key name must be PRIMARY in MySQL");
        }
        sqlBuilder.append("PRIMARY KEY ");
        break;
      case UNIQUE_KEY:
        sqlBuilder
            .append("UNIQUE INDEX ")
            .append(BACK_QUOTE)
            .append(addIndex.getName())
            .append(BACK_QUOTE);
        break;
      default:
        break;
    }
    sqlBuilder.append(" (").append(getIndexFieldStr(addIndex.getFieldNames())).append(")");
    return sqlBuilder.toString();
  }

  private String generateTableProperties(List<TableChange.SetProperty> setProperties) {
    return setProperties.stream()
        .map(
            setProperty ->
                String.format("%s = %s", setProperty.getProperty(), setProperty.getValue()))
        .collect(Collectors.joining(",\n"));
  }

  protected JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    return null != lazyLoadCreateTable ? lazyLoadCreateTable : load(databaseName, tableName);
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, JdbcTable jdbcTable) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnComment.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    JdbcColumn updateColumn =
        new JdbcColumn.Builder()
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
    String dataType = (String) typeConverter.fromGravitinoType(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = addColumn.fieldName()[0];

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition
        .append("ADD COLUMN ")
        .append(BACK_QUOTE)
        .append(col)
        .append(BACK_QUOTE)
        .append(SPACE)
        .append(dataType)
        .append(SPACE);

    if (addColumn.isAutoIncrement()) {
      Preconditions.checkArgument(
          Types.allowAutoIncrement(addColumn.getDataType()),
          "Auto increment is not allowed, type: " + addColumn.getDataType());
      columnDefinition.append(MYSQL_AUTO_INCREMENT).append(SPACE);
    }

    if (!addColumn.isNullable()) {
      columnDefinition.append("NOT NULL ");
    }
    // Append comment if available
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
      // do nothing, follow the default behavior of mysql
    } else {
      throw new IllegalArgumentException("Invalid column position.");
    }
    return columnDefinition.toString();
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, JdbcTable jdbcTable) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String oldColumnName = renameColumn.fieldName()[0];
    String newColumnName = renameColumn.getNewName();
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, oldColumnName);
    StringBuilder sqlBuilder =
        new StringBuilder(
            "CHANGE COLUMN "
                + BACK_QUOTE
                + oldColumnName
                + BACK_QUOTE
                + SPACE
                + BACK_QUOTE
                + newColumnName
                + BACK_QUOTE);
    JdbcColumn newColumn =
        new JdbcColumn.Builder()
            .withName(newColumnName)
            .withType(column.dataType())
            .withComment(column.comment())
            .withDefaultValue(column.defaultValue())
            .withNullable(column.nullable())
            .withAutoIncrement(column.autoIncrement())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException(MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnPosition.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append(MODIFY_COLUMN).append(col);
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
      throw new UnsupportedOperationException(MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
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

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException(MYSQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder = new StringBuilder(MODIFY_COLUMN + col);
    JdbcColumn newColumn =
        new JdbcColumn.Builder()
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
    // Add data type
    sqlBuilder
        .append(SPACE)
        .append(typeConverter.fromGravitinoType(column.dataType()))
        .append(SPACE);

    // Add NOT NULL if the column is marked as such
    if (column.nullable()) {
      sqlBuilder.append("NULL ");
    } else {
      sqlBuilder.append("NOT NULL ");
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }

    // Add column auto_increment if specified
    if (column.autoIncrement()) {
      sqlBuilder.append(MYSQL_AUTO_INCREMENT).append(" ");
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
  }
}
