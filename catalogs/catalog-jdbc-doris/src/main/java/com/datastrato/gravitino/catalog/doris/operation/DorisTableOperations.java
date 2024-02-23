/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.doris.utils.DorisUtils;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for Doris. */
public class DorisTableOperations extends JdbcTableOperations {
  public static final String BACK_QUOTE = "`";
  public static final String DORIS_AUTO_INCREMENT = "AUTO_INCREMENT";

  public static final String NEW_LINE = "\n";

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
      com.datastrato.gravitino.rel.indexes.Index[] indexes) {

    validateIncrementCol(columns);

    StringBuilder sqlBuilder = new StringBuilder();

    sqlBuilder.append(String.format("CREATE TABLE `%s` (", tableName)).append(NEW_LINE);

    // Add columns
    sqlBuilder.append(
        Arrays.stream(columns)
            .map(
                column -> {
                  StringBuilder columnsSql = new StringBuilder();
                  columnsSql
                      .append(SPACE)
                      .append(BACK_QUOTE)
                      .append(column.name())
                      .append(BACK_QUOTE);
                  appendColumnDefinition(column, columnsSql);
                  return columnsSql.toString();
                })
            .collect(Collectors.joining(",\n")));

    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append(NEW_LINE).append(")");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder.append(" COMMENT \"").append(comment).append("\"");
    }

    // Add table properties
    sqlBuilder.append(DorisUtils.generatePropertiesSql(properties));

    // Add Partition Info
    if (partitioning != null && partitioning.length > 0) {
      // TODO: Add partitioning support
    }

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  private static void validateIncrementCol(JdbcColumn[] columns) {
    // Check auto increment column
    List<JdbcColumn> autoIncrementCols =
        Arrays.stream(columns).filter(Column::autoIncrement).collect(Collectors.toList());
    String autoIncrementColsStr =
        autoIncrementCols.stream().map(JdbcColumn::name).collect(Collectors.joining(",", "[", "]"));

    Preconditions.checkArgument(
        autoIncrementCols.size() <= 1,
        "Only one column can be auto-incremented. There are multiple auto-increment columns in your table: "
            + autoIncrementColsStr);

    // Check Auto increment column type
    autoIncrementCols.forEach(
        column -> {
          Preconditions.checkArgument(
              column.dataType().equals(Types.LongType.get()),
              "Auto-increment column must be of type BIGINT. The column "
                  + column.name()
                  + " is of type "
                  + column.dataType());
        });

    // Check auto increment column is nullable
    autoIncrementCols.forEach(
        column -> {
          Preconditions.checkArgument(
              !column.nullable(),
              "Auto-increment column must be not nullable. The column "
                  + column.name()
                  + " is nullable");
        });
  }

  @VisibleForTesting
  static void appendIndexesSql(
      com.datastrato.gravitino.rel.indexes.Index[] indexes, StringBuilder sqlBuilder) {

    // validate indexes
    Arrays.stream(indexes)
        .map(
            index -> {
              if (index.fieldNames().length > 1) {
                throw new IllegalArgumentException("Index does not support multi fields in Doris");
              }
              return index;
            });

    String indexSql =
        Arrays.stream(indexes)
            .map(index -> String.format("INDEX %s (%s)", index.name(), index.fieldNames()[0][0]))
            .collect(Collectors.joining(",\n"));

    sqlBuilder.append(indexSql);
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {

    String showCreateTableSQL = String.format("SHOW CREATE TABLE `%s`", tableName);

    StringBuilder createTableSqlSb = new StringBuilder();
    try (PreparedStatement statement = connection.prepareStatement(showCreateTableSQL)) {
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          createTableSqlSb.append(resultSet.getString("Create Table"));
        }
      }
    }

    String createTableSql = createTableSqlSb.toString();

    if (StringUtils.isEmpty(createTableSql)) {
      throw new NoSuchTableException(
          "Table %s does not exist in %s.", tableName, connection.getCatalog());
    }

    return Collections.unmodifiableMap(extractTablePropertiesFromSql(createTableSql));
  }

  @VisibleForTesting
  static Map<String, String> extractTablePropertiesFromSql(String createTableSql) {
    Map<String, String> properties = new HashMap<>();
    String[] lines = createTableSql.split("\n");

    boolean isProperties = false;
    final String sProperties = "\"(.*)\"\\s{0,}=\\s{0,}\"(.*)\",?";
    final Pattern patternProperties = Pattern.compile(sProperties);

    for (String line : lines) {
      if (line.contains("PROPERTIES")) {
        isProperties = true;
      }

      if (isProperties) {
        final Matcher matcherProperties = patternProperties.matcher(line);
        if (matcherProperties.find()) {
          final String key = matcherProperties.group(1).trim();
          String value = matcherProperties.group(2).trim();
          properties.put(key, value);
        }
      }
    }
    return properties;
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return String.format("ALTER TABLE `%s` RENAME `%s`", oldTableName, newTableName);
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return String.format("DROP TABLE `%s`", tableName);
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    return String.format("TRUNCATE TABLE `%s`", tableName);
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
        // Doris only support set properties, remove property is not supported yet
        throw new IllegalArgumentException("Remove property is not supported yet");
      } else if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addColumnFieldDefinition(addColumn));
      } else if (change instanceof TableChange.RenameColumn) {
        throw new IllegalArgumentException("Rename column is not supported yet");
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
      alterSql.add("COMMENT \"" + newComment + "\"");
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
    return "MODIFY COLUMN "
        + BACK_QUOTE
        + col
        + BACK_QUOTE
        + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  private String generateTableProperties(List<TableChange.SetProperty> setProperties) {
    return setProperties.stream()
        .map(
            setProperty ->
                String.format("\"%s\" = \"%s\"", setProperty.getProperty(), setProperty.getValue()))
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
      throw new UnsupportedOperationException("Doris does not support nested column names.");
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
    return "MODIFY COLUMN "
        + BACK_QUOTE
        + col
        + BACK_QUOTE
        + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    String dataType = (String) typeConverter.fromGravitinoType(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("Doris does not support nested column names.");
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
          .append("AFTER ")
          .append(BACK_QUOTE)
          .append(afterPosition.getColumn())
          .append(BACK_QUOTE);
    } else if (addColumn.getPosition() instanceof TableChange.Default) {
      // do nothing, follow the default behavior of doris
    } else {
      throw new IllegalArgumentException("Invalid column position.");
    }
    return columnDefinition.toString();
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException("Doris does not support nested column names.");
    }
    String col = updateColumnPosition.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append("MODIFY COLUMN ").append(col);
    appendColumnDefinition(column, columnDefinition);
    if (updateColumnPosition.getPosition() instanceof TableChange.First) {
      columnDefinition.append("FIRST");
    } else if (updateColumnPosition.getPosition() instanceof TableChange.After) {
      TableChange.After afterPosition = (TableChange.After) updateColumnPosition.getPosition();
      columnDefinition.append("AFTER ").append(afterPosition.getColumn());
    } else {
      Arrays.stream(jdbcTable.columns())
          .reduce((column1, column2) -> column2)
          .map(Column::name)
          .ifPresent(s -> columnDefinition.append("AFTER ").append(s));
    }
    return columnDefinition.toString();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable jdbcTable) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("Doris does not support nested column names.");
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
      throw new UnsupportedOperationException("Doris does not support nested column names.");
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder = new StringBuilder("MODIFY COLUMN " + col);
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
      sqlBuilder.append(DORIS_AUTO_INCREMENT).append(" ");
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
  }
}
