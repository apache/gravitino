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
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for Doris. */
public class DorisTableOperations extends JdbcTableOperations {
  private static final String BACK_QUOTE = "`";
  private static final String DORIS_AUTO_INCREMENT = "AUTO_INCREMENT";

  private static final String NEW_LINE = "\n";

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    final List<String> names = Lists.newArrayList();

    try (Connection connection = getConnection(databaseName);
        ResultSet tables = getTables(connection)) {
      while (tables.next()) {
        if (Objects.equals(tables.getString("TABLE_CAT"), databaseName)) {
          names.add(tables.getString("TABLE_NAME"));
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), databaseName);
      return names;
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
      Distribution distribution,
      com.datastrato.gravitino.rel.indexes.Index[] indexes) {

    validateIncrementCol(columns);
    validateDistribution(distribution, columns);

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

    // Add distribution info
    if (distribution.strategy() == Strategy.HASH) {
      sqlBuilder.append(NEW_LINE).append(" DISTRIBUTED BY HASH(");
      sqlBuilder.append(
          Arrays.stream(distribution.expressions())
              .map(column -> BACK_QUOTE + column.toString() + BACK_QUOTE)
              .collect(Collectors.joining(", ")));
      sqlBuilder.append(")");
    } else if (distribution.strategy() == Strategy.EVEN) {
      sqlBuilder.append(NEW_LINE).append(" DISTRIBUTED BY ").append("RANDOM");
    }

    if (distribution.number() != 0) {
      sqlBuilder.append(" BUCKETS ").append(distribution.number());
    }

    // Add table properties
    sqlBuilder.append(NEW_LINE).append(DorisUtils.generatePropertiesSql(properties));

    // Add Partition Info
    if (partitioning != null && partitioning.length > 0) {
      // TODO: Add partitioning support
      throw new UnsupportedOperationException("Currently we do not support Partitioning in Doris");
    }

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  private static void validateIncrementCol(JdbcColumn[] columns) {
    // Get all auto increment column
    List<JdbcColumn> autoIncrementCols =
        Arrays.stream(columns).filter(Column::autoIncrement).collect(Collectors.toList());

    // Doris does not support auto increment column before version 2.1.0
    Preconditions.checkArgument(
        autoIncrementCols.isEmpty(), "Doris does not support auto-increment column");
  }

  private static void validateDistribution(Distribution distribution, JdbcColumn[] columns) {
    Preconditions.checkArgument(null != distribution, "Doris must set distribution");

    Preconditions.checkArgument(
        Strategy.HASH == distribution.strategy() || Strategy.EVEN == distribution.strategy(),
        "Doris only supports HASH or EVEN distribution strategy");

    if (distribution.strategy() == Strategy.HASH) {
      // Check if the distribution column exists
      Arrays.stream(distribution.expressions())
          .forEach(
              expression -> {
                Preconditions.checkArgument(
                    Arrays.stream(columns)
                        .anyMatch(column -> column.name().equalsIgnoreCase(expression.toString())),
                    "Distribution column " + expression + " does not exist in the table columns");
              });
    }
  }

  @VisibleForTesting
  static void appendIndexesSql(
      com.datastrato.gravitino.rel.indexes.Index[] indexes, StringBuilder sqlBuilder) {

    if (indexes.length == 0) {
      return;
    }

    // validate indexes
    Arrays.stream(indexes)
        .forEach(
            index -> {
              if (index.fieldNames().length > 1) {
                throw new IllegalArgumentException("Index does not support multi fields in Doris");
              }
            });

    String indexSql =
        Arrays.stream(indexes)
            .map(index -> String.format("INDEX %s (%s)", index.name(), index.fieldNames()[0][0]))
            .collect(Collectors.joining(",\n"));

    sqlBuilder.append(",").append(NEW_LINE).append(indexSql);
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT"));
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {

    String showCreateTableSQL = String.format("SHOW CREATE TABLE `%s`", tableName);

    StringBuilder createTableSqlSb = new StringBuilder();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(showCreateTableSQL)) {
      while (resultSet.next()) {
        createTableSqlSb.append(resultSet.getString("Create Table"));
      }
    }

    String createTableSql = createTableSqlSb.toString();

    if (StringUtils.isEmpty(createTableSql)) {
      throw new NoSuchTableException(
          "Table %s does not exist in %s.", tableName, connection.getCatalog());
    }

    return Collections.unmodifiableMap(DorisUtils.extractPropertiesFromSql(createTableSql));
  }

  @Override
  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName)
      throws SQLException {
    String sql = String.format("SHOW INDEX FROM `%s` FROM `%s`", tableName, databaseName);

    // get Indexes from SQL
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      List<Index> indexes = new ArrayList<>();
      while (resultSet.next()) {
        String indexName = resultSet.getString("Key_name");
        String columnName = resultSet.getString("Column_name");
        indexes.add(
            Indexes.of(Index.IndexType.PRIMARY_KEY, indexName, new String[][] {{columnName}}));
      }
      return indexes;
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  @Override
  protected void correctJdbcTableFields(
      Connection connection, String databaseName, String tableName, JdbcTable.Builder tableBuilder)
      throws SQLException {
    if (StringUtils.isNotEmpty(tableBuilder.comment())) {
      return;
    }

    // Doris Cannot get comment from JDBC 8.x, so we need to get comment from sql
    StringBuilder comment = new StringBuilder();
    String sql =
        "SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setString(1, databaseName);
      preparedStatement.setString(2, tableName);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          comment.append(resultSet.getString("TABLE_COMMENT"));
        }
      }
      tableBuilder.withComment(comment.toString());
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }

    getTableStatus(connection, databaseName, tableName);
  }

  protected void getTableStatus(Connection connection, String databaseName, String tableName) {
    // sql is `SHOW ALTER TABLE COLUMN WHERE TableName = 'test_table'`
    // database name must be specified in connection, so the SQL do not need to specify database
    // name
    String sql =
        String.format(
            "SHOW ALTER TABLE COLUMN WHERE TableName = '%s' ORDER BY JobId DESC limit 1",
            tableName);

    // Just print each column name and type from resultSet
    // TODO: add to table properties or other fields
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      StringBuilder jobStatus = new StringBuilder();
      while (resultSet.next()) {
        int columnCount = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          jobStatus
              .append(resultSet.getMetaData().getColumnName(i))
              .append(" : ")
              .append(resultSet.getString(i))
              .append(", ");
        }
        jobStatus.append(" | ");
      }

      if (jobStatus.length() > 0) {
        LOG.info(
            "Table {}.{} schema-change execution status: {}",
            databaseName,
            tableName,
            jobStatus.toString());
      }

    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
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
    throw new UnsupportedOperationException(
        "Doris does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    /*
     * NOTICE:
     * As described in the Doris documentation, the creation of Schema Change is an asynchronous process.
     * If you load the table immediately after altering it, you might get the old schema.
     * You can see in: https://doris.apache.org/docs/1.2/advanced/alter-table/schema-change/#create-job
     * TODO: return state of the operation to user
     * */

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
        TableChange.UpdateColumnComment updateColumnComment =
            (TableChange.UpdateColumnComment) change;
        alterSql.add(updateColumnCommentFieldDefinition(updateColumnComment));
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
      alterSql.add("MODIFY COMMENT \"" + newComment + "\"");
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
        JdbcColumn.builder()
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
      TableChange.UpdateColumnComment updateColumnComment) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException("Doris does not support nested column names.");
    }
    String col = updateColumnComment.fieldName()[0];

    return String.format("MODIFY COLUMN `%s` COMMENT '%s'", col, newComment);
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
    columnDefinition.append("MODIFY COLUMN ").append(BACK_QUOTE).append(col).append(BACK_QUOTE);
    appendColumnDefinition(column, columnDefinition);
    if (updateColumnPosition.getPosition() instanceof TableChange.First) {
      columnDefinition.append("FIRST");
    } else if (updateColumnPosition.getPosition() instanceof TableChange.After) {
      TableChange.After afterPosition = (TableChange.After) updateColumnPosition.getPosition();
      columnDefinition
          .append("AFTER ")
          .append(BACK_QUOTE)
          .append(afterPosition.getColumn())
          .append(BACK_QUOTE);
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
    StringBuilder sqlBuilder = new StringBuilder("MODIFY COLUMN " + BACK_QUOTE + col + BACK_QUOTE);
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

  static String addIndexDefinition(TableChange.AddIndex addIndex) {
    return String.format("ADD INDEX %s (%s)", addIndex.getName(), addIndex.getFieldNames()[0][0]);
  }

  static String deleteIndexDefinition(
      JdbcTable lazyLoadTable, TableChange.DeleteIndex deleteIndex) {
    if (deleteIndex.isIfExists()) {
      Preconditions.checkArgument(
          Arrays.stream(lazyLoadTable.index())
              .anyMatch(index -> index.name().equals(deleteIndex.getName())),
          "Index does not exist");
    }
    return "DROP INDEX " + deleteIndex.getName();
  }
}
