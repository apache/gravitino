/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.operation;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for MySQL. */
public class MysqlTableOperations extends JdbcTableOperations {

  public static final String BACK_QUOTE = "`";

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    List<JdbcColumn> jdbcColumns = new ArrayList<>();
    try (Connection connection = getConnection(databaseName)) {
      // 1.Get table information
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet table = metaData.getTables(databaseName, null, tableName, null);
      if (!table.next() || !tableName.equals(table.getString("TABLE_NAME"))) {
        throw new NoSuchTableException(
            String.format("Table %s does not exist in %s.", tableName, databaseName));
      }
      String comment = table.getString("REMARKS");

      // 2.Get column information
      ResultSet columns = metaData.getColumns(databaseName, null, tableName, null);
      while (columns.next()) {
        MysqlTypeConverter.MysqlTypeBean typeBean =
            new MysqlTypeConverter.MysqlTypeBean(columns.getString("TYPE_NAME"));
        typeBean.setColumnSize(columns.getString("COLUMN_SIZE"));
        typeBean.setScale(columns.getString("DECIMAL_DIGITS"));

        String columnName = columns.getString("COLUMN_NAME");
        String colComment = columns.getString("REMARKS");
        jdbcColumns.add(
            new JdbcColumn.Builder()
                .withName(columnName)
                .withType(typeConverter.toGravitinoType(typeBean))
                .withNullable(columns.getBoolean("NULLABLE"))
                .withComment(StringUtils.isEmpty(colComment) ? null : colComment)
                // TODO #1531 will add default value.
                //                .withDefaultValue(columns.getString("COLUMN_DEF"))
                .withProperties(Collections.emptyList())
                .build());
      }

      return new JdbcTable.Builder()
          .withName(tableName)
          .withColumns(jdbcColumns.toArray(new JdbcColumn[0]))
          .withComment(comment)
          .withProperties(Collections.emptyMap())
          .withAuditInfo(AuditInfo.EMPTY)
          .build();
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
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

  private JdbcColumn getJdbcColumnFromTable(JdbcTable jdbcTable, String colName) {
    return (JdbcColumn)
        Arrays.stream(jdbcTable.columns())
            .filter(column -> column.name().equals(colName))
            .findFirst()
            .orElseThrow(
                () ->
                    new NoSuchColumnException(
                        "Column " + colName + " does not exist in table " + jdbcTable.name()));
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
    return "DROP TABLE " + tableName;
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

    if (CollectionUtils.isEmpty(alterSql)) {
      return "";
    }
    // Return the generated SQL statement
    String result = "ALTER TABLE " + tableName + "\n" + String.join(",\n", alterSql) + ";";
    LOG.info("Generated alter table:{} sql: {}", databaseName + "." + tableName, result);
    return result;
  }

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability change, JdbcTable table) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = change.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(table, col);
    JdbcColumn updateColumn =
        new JdbcColumn.Builder()
            .withName(col)
            // TODO #1531 will add default value.
            .withDefaultValue(null)
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

  private JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    return null != lazyLoadCreateTable ? lazyLoadCreateTable : load(databaseName, tableName);
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, JdbcTable jdbcTable) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = updateColumnComment.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    JdbcColumn updateColumn =
        new JdbcColumn.Builder()
            .withName(col)
            // TODO #1531 will add default value.
            .withDefaultValue(null)
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
      TableChange.RenameColumn renameColumn, JdbcTable jdbcTable) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }

    String oldColumnName = renameColumn.fieldName()[0];
    String newColumnName = renameColumn.getNewName();
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, oldColumnName);
    StringBuilder sqlBuilder =
        new StringBuilder("CHANGE COLUMN " + oldColumnName + SPACE + newColumnName);
    JdbcColumn newColumn =
        new JdbcColumn.Builder()
            .withName(newColumnName)
            .withType(column.dataType())
            .withComment(column.comment())
            .withProperties(column.getProperties())
            // TODO #1531 will add default value.
            .withDefaultValue(null)
            .withNullable(column.nullable())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
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
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
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
    return "DROP COLUMN " + col;
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException("Mysql does not support nested column names.");
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
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
    // TODO #1531 will add default value.

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
}
