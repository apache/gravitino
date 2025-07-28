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
package org.apache.gravitino.catalog.doris.operation;

import static org.apache.gravitino.catalog.doris.DorisCatalog.DORIS_TABLE_PROPERTIES_META;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.DEFAULT_REPLICATION_FACTOR_IN_SERVER_SIDE;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.REPLICATION_FACTOR;
import static org.apache.gravitino.catalog.doris.utils.DorisUtils.generatePartitionSqlFragment;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.doris.utils.DorisUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.RangePartition;

/** Table operations for Apache Doris. */
public class DorisTableOperations extends JdbcTableOperations {
  private static final String BACK_QUOTE = "`";
  private static final String DORIS_AUTO_INCREMENT = "AUTO_INCREMENT";
  private static final String NEW_LINE = "\n";

  @Override
  public JdbcTablePartitionOperations createJdbcTablePartitionOperations(JdbcTable loadedTable) {
    return new DorisTablePartitionOperations(
        dataSource, loadedTable, exceptionMapper, typeConverter);
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

    // Add Partition Info
    appendPartitionSql(partitioning, columns, sqlBuilder);

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
      sqlBuilder.append(" BUCKETS ").append(DorisUtils.toBucketNumberString(distribution.number()));
    }

    properties = appendNecessaryProperties(properties);
    // Add table properties
    sqlBuilder.append(NEW_LINE).append(DorisUtils.generatePropertiesSql(properties));

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  private Map<String, String> appendNecessaryProperties(Map<String, String> properties) {
    Map<String, String> resultMap;
    if (properties == null) {
      resultMap = new HashMap<>();
    } else {
      resultMap = new HashMap<>(properties);
    }

    // If the backend server is less than DEFAULT_REPLICATION_FACTOR_IN_SERVER_SIDE (3), we need to
    // set the property 'replication_num' to 1 explicitly.
    if (!resultMap.containsKey(REPLICATION_FACTOR)) {
      // Try to check the number of backend servers using `show backends`, this SQL is supported by
      // all versions of Doris
      String query = "show backends";

      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(query)) {
        int backendCount = 0;
        while (resultSet.next()) {
          String alive = resultSet.getString("Alive");
          if ("true".equalsIgnoreCase(alive)) {
            backendCount++;
          }
        }
        if (backendCount < DEFAULT_REPLICATION_FACTOR_IN_SERVER_SIDE) {
          resultMap.put(
              REPLICATION_FACTOR,
              DORIS_TABLE_PROPERTIES_META
                  .propertyEntries()
                  .get(REPLICATION_FACTOR)
                  .getDefaultValue()
                  .toString());
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to get the number of backend servers", e);
      }
    }

    return resultMap;
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
        "Doris only supports HASH or EVEN(RANDOM) distribution strategy");

    if (distribution.strategy() == Strategy.HASH) {
      // Check if the distribution column exists
      Arrays.stream(distribution.expressions())
          .forEach(
              expression ->
                  Preconditions.checkArgument(
                      Arrays.stream(columns)
                          .anyMatch(
                              column -> column.name().equalsIgnoreCase(expression.toString())),
                      "Distribution column "
                          + expression
                          + " does not exist in the table columns"));
    } else if (distribution.strategy() == Strategy.EVEN) {
      Preconditions.checkArgument(
          distribution.expressions().length == 0,
          "Doris does not support distribution column in EVEN distribution strategy");
    }
  }

  private static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
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

  private static void appendPartitionSql(
      Transform[] partitioning, JdbcColumn[] columns, StringBuilder sqlBuilder) {
    if (ArrayUtils.isEmpty(partitioning)) {
      return;
    }
    Preconditions.checkArgument(
        partitioning.length == 1, "Composite partition type is not supported");

    StringBuilder partitionSqlBuilder;
    Set<String> columnNames =
        Arrays.stream(columns).map(JdbcColumn::name).collect(Collectors.toSet());

    if (partitioning[0] instanceof Transforms.RangeTransform) {
      // We do not support multi-column range partitioning in doris for now
      Transforms.RangeTransform rangePartition = (Transforms.RangeTransform) partitioning[0];
      partitionSqlBuilder = generateRangePartitionSql(rangePartition, columnNames);
    } else if (partitioning[0] instanceof Transforms.ListTransform) {
      Transforms.ListTransform listPartition = (Transforms.ListTransform) partitioning[0];
      partitionSqlBuilder = generateListPartitionSql(listPartition, columnNames);
    } else {
      throw new IllegalArgumentException("Unsupported partition type of Doris");
    }

    sqlBuilder.append(partitionSqlBuilder);
  }

  private static StringBuilder generateRangePartitionSql(
      Transforms.RangeTransform rangePartition, Set<String> columnNames) {
    Preconditions.checkArgument(
        rangePartition.fieldName().length == 1, "Doris partition does not support nested field");
    Preconditions.checkArgument(
        columnNames.contains(rangePartition.fieldName()[0]),
        "The partition field must be one of the columns");

    StringBuilder partitionSqlBuilder = new StringBuilder(NEW_LINE);
    String partitionDefinition =
        String.format(" PARTITION BY RANGE(`%s`)", rangePartition.fieldName()[0]);
    partitionSqlBuilder.append(partitionDefinition).append(NEW_LINE).append("(");

    // Assign range partitions
    RangePartition[] assignments = rangePartition.assignments();
    if (!ArrayUtils.isEmpty(assignments)) {
      String partitionSqlFragments =
          Arrays.stream(assignments)
              .map(DorisUtils::generatePartitionSqlFragment)
              .collect(Collectors.joining("," + NEW_LINE));
      partitionSqlBuilder.append(NEW_LINE).append(partitionSqlFragments);
    }

    partitionSqlBuilder.append(NEW_LINE).append(")");
    return partitionSqlBuilder;
  }

  private static StringBuilder generateListPartitionSql(
      Transforms.ListTransform listPartition, Set<String> columnNames) {
    ImmutableList.Builder<String> partitionColumnsBuilder = ImmutableList.builder();
    String[][] filedNames = listPartition.fieldNames();
    for (String[] filedName : filedNames) {
      Preconditions.checkArgument(
          filedName.length == 1, "Doris partition does not support nested field");
      Preconditions.checkArgument(
          columnNames.contains(filedName[0]), "The partition field must be one of the columns");

      partitionColumnsBuilder.add(BACK_QUOTE + filedName[0] + BACK_QUOTE);
    }
    String partitionColumns =
        partitionColumnsBuilder.build().stream().collect(Collectors.joining(","));

    StringBuilder partitionSqlBuilder = new StringBuilder(NEW_LINE);
    String partitionDefinition = String.format(" PARTITION BY LIST(%s)", partitionColumns);
    partitionSqlBuilder.append(partitionDefinition).append(NEW_LINE).append("(");

    // Assign list partitions
    ListPartition[] assignments = listPartition.assignments();
    if (!ArrayUtils.isEmpty(assignments)) {
      ImmutableList.Builder<String> partitions = ImmutableList.builder();
      for (ListPartition part : assignments) {
        Literal<?>[][] lists = part.lists();
        Preconditions.checkArgument(
            lists.length > 0, "The number of values in list partition must be greater than 0");
        Preconditions.checkArgument(
            Arrays.stream(lists).allMatch(p -> p.length == filedNames.length),
            "The number of partitioning columns must be consistent");

        partitions.add(generatePartitionSqlFragment(part));
      }
      partitionSqlBuilder
          .append(NEW_LINE)
          .append(partitions.build().stream().collect(Collectors.joining("," + NEW_LINE)));
    }

    partitionSqlBuilder.append(NEW_LINE).append(")");
    return partitionSqlBuilder;
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
  protected Transform[] getTablePartitioning(
      Connection connection, String databaseName, String tableName) throws SQLException {
    String showCreateTableSql = String.format("SHOW CREATE TABLE `%s`", tableName);
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(showCreateTableSql)) {
      StringBuilder createTableSql = new StringBuilder();
      if (result.next()) {
        createTableSql.append(result.getString("Create Table"));
      }
      Optional<Transform> transform =
          DorisUtils.extractPartitionInfoFromSql(createTableSql.toString());
      return transform.map(t -> new Transform[] {t}).orElse(Transforms.EMPTY_TRANSFORM);
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
            "Table {}.{} schema-change execution status: {}", databaseName, tableName, jobStatus);
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
        // Detect and add Gravitino id.
        JdbcTable jdbcTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
      alterSql.add("MODIFY COMMENT \"" + newComment + "\"");
    }

    if (CollectionUtils.isEmpty(alterSql)) {
      return "";
    }
    // Return the generated SQL statement
    String result = "ALTER TABLE `" + tableName + "`\n" + String.join(",\n", alterSql) + ";";
    LOG.info("Generated alter table:{}.{} sql: {}", databaseName, tableName, result);
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
    String properties =
        setProperties.stream()
            .map(
                setProperty ->
                    String.format(
                        "\"%s\" = \"%s\"", setProperty.getProperty(), setProperty.getValue()))
            .collect(Collectors.joining(",\n"));
    return "set (" + properties + ")";
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
    String dataType = typeConverter.fromGravitino(addColumn.getDataType());
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
    sqlBuilder.append(SPACE).append(typeConverter.fromGravitino(column.dataType())).append(SPACE);

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
    if (!deleteIndex.isIfExists()) {
      Preconditions.checkArgument(
          Arrays.stream(lazyLoadTable.index())
              .anyMatch(index -> index.name().equals(deleteIndex.getName())),
          "Index does not exist");
    }
    return "DROP INDEX " + deleteIndex.getName();
  }

  @Override
  protected Distribution getDistributionInfo(
      Connection connection, String databaseName, String tableName) throws SQLException {

    String showCreateTableSql = String.format("SHOW CREATE TABLE `%s`", tableName);
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(showCreateTableSql)) {
      result.next();
      String createTableSyntax = result.getString("Create Table");
      return DorisUtils.extractDistributionInfoFromSql(createTableSyntax);
    }
  }

  @Override
  public Integer calculateDatetimePrecision(String typeName, int columnSize, int scale) {
    String upperTypeName = typeName.toUpperCase();

    // Check driver version compatibility first
    boolean isDatetimeType = "DATETIME".equals(upperTypeName);

    if (isDatetimeType) {
      String driverVersion = getMySQLDriverVersion();
      if (driverVersion != null && !isMySQLDriverVersionSupported(driverVersion)) {
        LOG.warn(
            "MySQL driver version {} is below 8.0.16, columnSize may not be accurate for precision calculation. "
                + "Returning null for {} type precision. Driver version: {}",
            driverVersion,
            upperTypeName,
            driverVersion);
        return null;
      }
    }

    if (upperTypeName.equals("DATETIME")) {
      // DATETIME format: 'YYYY-MM-DD HH:MM:SS' (19 chars) + decimal point + precision
      return columnSize >= DATETIME_FORMAT_WITH_DOT.length()
          ? columnSize - DATETIME_FORMAT_WITH_DOT.length()
          : 0;
    }
    return null;
  }
}
