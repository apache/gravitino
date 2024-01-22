/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.operation;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for MySQL. */
public class MysqlTableOperations extends JdbcTableOperations {

  public static final String AUTO_INCREMENT = "AUTO_INCREMENT";
  public static final String BACK_QUOTE = "`";

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    CreateTable createTable = loadCreateTable(databaseName, tableName);
    List<JdbcColumn> jdbcColumns = new ArrayList<>();
    // Assemble index information.
    Map<String, Set<String>> indexGroupByName =
        getIndexNameGroupByColumnName(createTable.getIndexes());
    for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
      // Assemble column information.
      String columnName = getColumnName(columnDefinition);
      String[] columnSpecs =
          columnDefinition.getColumnSpecs() == null
              ? new String[0]
              : columnDefinition.getColumnSpecs().toArray(new String[0]);
      String columnProperties = String.join(SPACE, columnSpecs);
      boolean nullable = !columnProperties.contains(NOT_NULL);
      String defaultValue = findPropertiesValue(columnSpecs, DEFAULT);
      String comment = findPropertiesValue(columnSpecs, COMMENT);
      List<String> properties = getColumnProperties(columnProperties);
      Optional.ofNullable(indexGroupByName.get(columnName)).ifPresent(properties::addAll);
      jdbcColumns.add(
          new JdbcColumn.Builder()
              .withName(columnName)
              .withType(typeConverter.toGravitinoType(columnDefinition.getColDataType()))
              .withNullable(nullable)
              .withComment(comment)
              // TODO: uncomment this once we support column default values.
              // .withDefaultValue("NULL".equals(defaultValue) ? null : defaultValue)
              .withProperties(properties)
              .build());
    }
    Map<String, String> properties =
        parseOrderedKeyValuePairs(createTable.getTableOptionsStrings().toArray(new String[0]));

    String remove = properties.remove(COMMENT);
    return new JdbcTable.Builder()
        .withName(tableName)
        .withColumns(jdbcColumns.toArray(new JdbcColumn[0]))
        .withComment(remove)
        .withProperties(properties)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

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

  private JdbcColumn getJdbcColumnFromCreateTable(CreateTable createTable, String colName) {
    // Assemble index information.
    Map<String, Set<String>> indexGroupByName =
        getIndexNameGroupByColumnName(createTable.getIndexes());
    for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
      // Assemble column information.
      String columnName = getColumnName(columnDefinition);
      if (!StringUtils.equals(colName, columnName)) {
        continue;
      }
      String[] columnSpecs =
          columnDefinition.getColumnSpecs() == null
              ? new String[0]
              : columnDefinition.getColumnSpecs().toArray(new String[0]);
      String columnProperties = String.join(SPACE, columnSpecs);
      boolean nullable = !columnProperties.contains(NOT_NULL);
      String defaultValue = findPropertiesValue(columnSpecs, DEFAULT);
      String comment = findPropertiesValue(columnSpecs, COMMENT);
      List<String> properties = getColumnProperties(columnProperties);
      Optional.ofNullable(indexGroupByName.get(columnName)).ifPresent(properties::addAll);
      return new JdbcColumn.Builder()
          .withName(columnName)
          .withType(typeConverter.toGravitinoType(columnDefinition.getColDataType()))
          .withNullable(nullable)
          .withComment(comment)
          // TODO: uncomment this once we support column default values.
          // .withDefaultValue("NULL".equals(defaultValue) ? null : defaultValue)
          .withProperties(properties)
          .build();
    }
    throw new NoSuchColumnException(
        "Column " + colName + " does not exist in table " + createTable.getTable().getName());
  }

  /**
   * @param databaseName database name
   * @param tableName table name
   * @return create table statement
   */
  private CreateTable loadCreateTable(String databaseName, String tableName) {
    try (Connection connection = getConnection(databaseName)) {
      try (Statement statement = connection.createStatement()) {
        String showCreateTableSQL = String.format("SHOW CREATE TABLE %s", tableName);
        ResultSet resultSet = statement.executeQuery(showCreateTableSQL);

        if (!resultSet.next()) {
          throw new NoSuchTableException("Table " + tableName + " does not exist.");
        }
        String createTableSql = resultSet.getString(2);
        return (CreateTable) CCJSqlParserUtil.parse(createTableSql);
      } catch (JSQLParserException e) {
        throw new GravitinoRuntimeException(
            String.format("Failed to parse create table %s.%s sql", databaseName, tableName), e);
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * @param indexes table index information object. For example: KEY `idx_name` (`name`) USING BTREE
   * @return Get index information grouped by column name. For example: {name=[KEY, BTREE]}
   */
  private static Map<String, Set<String>> getIndexNameGroupByColumnName(List<Index> indexes) {
    return indexes == null
        ? Collections.emptyMap()
        : indexes.stream()
            .flatMap(
                index ->
                    index.getColumnsNames().stream()
                        .map(
                            s ->
                                new AbstractMap.SimpleEntry<String, Set<String>>(
                                    s.replaceAll("`", ""),
                                    new HashSet<String>() {
                                      {
                                        add(index.getType());
                                      }
                                    })))
            .collect(
                Collectors.toMap(
                    AbstractMap.SimpleEntry::getKey,
                    AbstractMap.SimpleEntry::getValue,
                    (set, other) -> {
                      set.addAll(other);
                      return set;
                    }));
  }

  private List<String> getColumnProperties(String columnProperties) {
    List<String> properties = new ArrayList<>();
    if (StringUtils.containsIgnoreCase(columnProperties, AUTO_INCREMENT)) {
      properties.add(AUTO_INCREMENT);
    }
    return properties;
  }

  @Override
  protected Map<String, String> extractPropertiesFromResultSet(ResultSet table) {
    // We have rewritten the `load` method, so there is no need to implement this method
    throw new UnsupportedOperationException("Extracting table properties is not supported yet");
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException("Currently we do not support Partitioning in mysql");
    }
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE ").append(tableName).append(" (\n");

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
    sqlBuilder.append("\n)");
    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      // TODO #804 Properties will be unified in the future.
      throw new UnsupportedOperationException("Properties are not supported yet");
      //      StringJoiner joiner = new StringJoiner(SPACE + SPACE);
      //      for (Map.Entry<String, String> entry : properties.entrySet()) {
      //        joiner.add(entry.getKey() + "=" + entry.getValue());
      //      }
      //      sqlBuilder.append(joiner);
    }

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder.append(" COMMENT='").append(comment).append("'");
    }

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @Override
  protected JdbcColumn extractJdbcColumnFromResultSet(ResultSet resultSet) {
    // We have rewritten the `load` method, so there is no need to implement this method
    throw new UnsupportedOperationException("Extracting table columns is not supported yet");
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return String.format("RENAME TABLE %s TO %s", oldTableName, newTableName);
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "MySQL does not support drop operation in Gravitino, please use purge operation");
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    return "DROP TABLE " + tableName;
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    // Not all operations require the original table information, so lazy loading is used here
    CreateTable lazyLoadCreateTable = null;
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
        alterSql.add(addColumnFieldDefinition(addColumn));
      } else if (change instanceof TableChange.RenameColumn) {
        lazyLoadCreateTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        alterSql.add(renameColumnFieldDefinition(renameColumn, lazyLoadCreateTable));
      } else if (change instanceof TableChange.UpdateColumnType) {
        lazyLoadCreateTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadCreateTable));
      } else if (change instanceof TableChange.UpdateColumnComment) {
        lazyLoadCreateTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        TableChange.UpdateColumnComment updateColumnComment =
            (TableChange.UpdateColumnComment) change;
        alterSql.add(updateColumnCommentFieldDefinition(updateColumnComment, lazyLoadCreateTable));
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        lazyLoadCreateTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        TableChange.UpdateColumnPosition updateColumnPosition =
            (TableChange.UpdateColumnPosition) change;
        alterSql.add(
            updateColumnPositionFieldDefinition(updateColumnPosition, lazyLoadCreateTable));
      } else if (change instanceof TableChange.DeleteColumn) {
        TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
        lazyLoadCreateTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        String deleteColSql = deleteColumnFieldDefinition(deleteColumn, lazyLoadCreateTable);
        if (StringUtils.isNotEmpty(deleteColSql)) {
          alterSql.add(deleteColSql);
        }
      } else if (change instanceof TableChange.UpdateColumnNullability) {
        lazyLoadCreateTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        alterSql.add(
            updateColumnNullabilityDefinition(
                (TableChange.UpdateColumnNullability) change, lazyLoadCreateTable));
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
        CreateTable createTable = getOrCreateTable(databaseName, tableName, lazyLoadCreateTable);
        Map<String, String> properties =
            parseOrderedKeyValuePairs(createTable.getTableOptionsStrings().toArray(new String[0]));
        StringIdentifier identifier = StringIdentifier.fromComment(properties.get(COMMENT));
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
      alterSql.add("COMMENT '" + newComment + "'");
    }

    if (CollectionUtils.isEmpty(alterSql)) {
      return "";
    }
    // Return the generated SQL statement
    String result = "ALTER TABLE " + tableName + "\n" + String.join(",\n", alterSql) + ";";
    LOG.info("Generated alter table:{} sql: {}", databaseName + "." + tableName, result);
    return result;
  }

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability change, CreateTable table) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = change.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromCreateTable(table, col);
    column.getProperties().remove(PRIMARY_KEY);
    JdbcColumn updateColumn =
        new JdbcColumn.Builder()
            .withName(col)
            // TODO: uncomment this once we support column default values.
            // .withDefaultValue(column.getDefaultValue())
            .withNullable(change.nullable())
            .withProperties(column.getProperties())
            .withType(column.dataType())
            .withComment(column.comment())
            .build();
    return "MODIFY COLUMN " + col + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  private String generateTableProperties(List<TableChange.SetProperty> setProperties) {
    // TODO #804 Properties will be unified in the future.
    //    return setProperties.stream()
    //        .map(
    //            setProperty ->
    //                String.format("%s = %s", setProperty.getProperty(), setProperty.getValue()))
    //        .collect(Collectors.joining(",\n"));
    return "";
  }

  private CreateTable getOrCreateTable(
      String databaseName, String tableName, CreateTable lazyLoadCreateTable) {
    return null != lazyLoadCreateTable
        ? lazyLoadCreateTable
        : loadCreateTable(databaseName, tableName);
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, CreateTable createTable) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = updateColumnComment.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromCreateTable(createTable, col);
    column.getProperties().remove(PRIMARY_KEY);
    JdbcColumn updateColumn =
        new JdbcColumn.Builder()
            .withName(col)
            // TODO: uncomment this once we support column default values.
            // .withDefaultValue(column.getDefaultValue())
            .withNullable(column.nullable())
            .withProperties(column.getProperties())
            .withType(column.dataType())
            .withComment(newComment)
            .build();
    return "MODIFY COLUMN " + col + appendColumnDefinition(updateColumn, new StringBuilder());
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    String dataType = (String) typeConverter.fromGravitinoType(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = addColumn.fieldName()[0];

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append("ADD COLUMN ").append(col).append(SPACE).append(dataType).append(SPACE);

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
      columnDefinition.append("AFTER ").append(afterPosition.getColumn());
    } else if (addColumn.getPosition() instanceof TableChange.Default) {
      // do nothing, follow the default behavior of mysql
    } else {
      throw new IllegalArgumentException("Invalid column position.");
    }
    return columnDefinition.toString();
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, CreateTable createTable) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }

    String oldColumnName = renameColumn.fieldName()[0];
    String newColumnName = renameColumn.getNewName();
    JdbcColumn column = getJdbcColumnFromCreateTable(createTable, oldColumnName);
    StringBuilder sqlBuilder =
        new StringBuilder("CHANGE COLUMN " + oldColumnName + SPACE + newColumnName);
    JdbcColumn newColumn =
        new JdbcColumn.Builder()
            .withName(newColumnName)
            .withType(column.dataType())
            .withComment(column.comment())
            .withProperties(column.getProperties())
            // TODO: uncomment this once we support column default values.
            // .withDefaultValue(column.getDefaultValue())
            .withNullable(column.nullable())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, CreateTable createTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = updateColumnPosition.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromCreateTable(createTable, col);
    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append("MODIFY COLUMN ").append(col);
    column.getProperties().remove(PRIMARY_KEY);
    appendColumnDefinition(column, columnDefinition);
    if (updateColumnPosition.getPosition() instanceof TableChange.First) {
      columnDefinition.append("FIRST");
    } else if (updateColumnPosition.getPosition() instanceof TableChange.After) {
      TableChange.After afterPosition = (TableChange.After) updateColumnPosition.getPosition();
      columnDefinition.append("AFTER ").append(afterPosition.getColumn());
    } else {
      List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
      if (CollectionUtils.isNotEmpty(columnDefinitions)) {
        columnDefinition
            .append("AFTER ")
            .append(getColumnName(columnDefinitions.get(columnDefinitions.size() - 1)));
      }
    }
    return columnDefinition.toString();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, CreateTable lazyLoadCreateTable) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = deleteColumn.fieldName()[0];
    boolean colExists =
        lazyLoadCreateTable.getColumnDefinitions().stream()
            .map(MysqlTableOperations::getColumnName)
            .anyMatch(s -> StringUtils.equals(col, s));
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column does not exist: " + col);
      }
    }
    return "DROP COLUMN " + col;
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, CreateTable createTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromCreateTable(createTable, col);
    StringBuilder sqlBuilder = new StringBuilder("MODIFY COLUMN " + col);
    JdbcColumn newColumn =
        new JdbcColumn.Builder()
            .withName(col)
            .withType(updateColumnType.getNewDataType())
            .withComment(column.comment())
            // Modifying a field type does not require adding its attributes. If
            // additional attributes are required, they must be modified separately.
            // TODO #839
            .withProperties(null)
            .withDefaultValue(null)
            .withNullable(column.nullable())
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
    // TODO: uncomment this once we support column default values.
    // if (StringUtils.isNotEmpty(column.getDefaultValue())) {
    //   sqlBuilder.append("DEFAULT '").append(column.getDefaultValue()).append("'").append(SPACE);
    // }

    // Add column properties if specified
    if (CollectionUtils.isNotEmpty(column.getProperties())) {
      for (String property : column.getProperties()) {
        sqlBuilder.append(property).append(SPACE);
      }
    }
    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
  }

  /**
   * @param input input string array. For example:
   *     ["ENGINE","=","InnoDB","DEFAULT","CHARSET","=","utf8mb4"]
   * @return key-value pairs. For example: {"ENGINE":"InnoDB","DEFAULT CHARSET":"utf8mb4"}
   */
  private static Map<String, String> parseOrderedKeyValuePairs(String[] input) {
    Map<String, String> keyValuePairs = new HashMap<>();
    parseOrderedKeyValuePairs(input, keyValuePairs);
    return keyValuePairs;
  }

  private static void parseOrderedKeyValuePairs(String[] input, Map<String, String> keyValuePairs) {
    if (input.length <= 1) {
      return;
    }
    int firstIndexOfEquals = ArrayUtils.indexOf(input, "=");
    if (-1 != firstIndexOfEquals) {
      // Found an equal sign, so this is a key-value pair
      String key = joinWords(input, 0, firstIndexOfEquals - 1);
      String value = input[firstIndexOfEquals + 1];

      // Check if the value is enclosed in single quotes
      if (value.startsWith("'") && value.endsWith("'")) {
        // Remove single quotes
        value = value.substring(1, value.length() - 1);
      }

      keyValuePairs.put(key, value);

      // Recursively process the remaining elements
      if (firstIndexOfEquals + 2 < input.length - 1) {
        parseOrderedKeyValuePairs(
            ArrayUtils.subarray(input, firstIndexOfEquals + 2, input.length), keyValuePairs);
      }
    }
  }

  private String findPropertiesValue(String[] columnSpecs, String propertyKey) {
    for (int i = 0; i < columnSpecs.length; i++) {
      String columnSpec = columnSpecs[i];
      if (propertyKey.equalsIgnoreCase(columnSpec) && i < columnSpecs.length - 1) {
        return columnSpecs[i + 1].replaceAll("'", SPACE).trim();
      }
    }
    return null;
  }

  private static String joinWords(String[] words, int start, int end) {
    StringBuilder result = new StringBuilder();
    for (int i = start; i <= end; i++) {
      if (i > start) {
        result.append(SPACE);
      }
      result.append(words[i]);
    }
    return result.toString();
  }

  private static String getColumnName(ColumnDefinition columnDefinition) {
    return columnDefinition.getColumnName().replaceAll("`", "");
  }
}
