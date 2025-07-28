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
package org.apache.gravitino.catalog.starrocks.operations;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.catalog.starrocks.utils.StarRocksUtils;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.RangePartition;

/** Table operations for StarRocks. */
public class StarRocksTableOperations extends JdbcTableOperations {

  private static final String BACK_QUOTE = "`";
  private static final String AUTO_INCREMENT = "AUTO_INCREMENT";
  private static final String NEW_LINE = "\n";

  private static final Set<String> SUPPORTED_MODIFY_PROPERTIES =
      new HashSet<>(
          Arrays.asList(
              "replication_num",
              "default.replication_num",
              "default.storage_medium",
              "enable_persistent_index",
              "bloom_filter_columns",
              "colocate_with",
              "bucket_size",
              "base_compaction_forbidden_time_ranges"));
  private static final String SUPPORTED_MODIFY_PROPERTIES_PREFIX_DYNAMIC_PARTITION =
      "dynamic_partition.";
  private static final String SUPPORTED_MODIFY_PROPERTIES_PREFIX_BINLOG = "binlog.";

  @Override
  public JdbcTablePartitionOperations createJdbcTablePartitionOperations(JdbcTable loadedTable) {
    return new StarRocksTablePartitionOperations(
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

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(String.format("CREATE TABLE `%s` ( \n", tableName));
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
    sqlBuilder.append(")\n");
    if (StringUtils.isNotEmpty(comment)) {
      comment = StringIdentifier.addToComment(StringIdentifier.DUMMY_ID, comment);
      sqlBuilder.append(" COMMENT \"").append(comment).append("\"");
    }

    appendPartitionSql(partitioning, columns, sqlBuilder);
    addDistributionSql(distribution, sqlBuilder);
    addPropertiesSql(properties, sqlBuilder);

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    JdbcTable lazyLoadTable = null;
    List<String> alterSql = new ArrayList<>();
    boolean hasSetPropertyChange = false;
    for (int i = 0; i < changes.length; i++) {
      TableChange change = changes[i];
      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addColumnFieldDefinition(addColumn));
      } else if (change instanceof TableChange.DeleteColumn) {
        TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(deleteColumnFieldDefinition(deleteColumn, lazyLoadTable));
      } else if (change instanceof TableChange.RemoveProperty) {
        throw new IllegalArgumentException("Remove property is not supported yet.");
      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(renameColumnDefinition(renameColumn, lazyLoadTable));
      } else if (change instanceof TableChange.RenameTable) {
        TableChange.RenameTable renameTable = (TableChange.RenameTable) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(renameTableDefinition(renameTable, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        TableChange.UpdateColumnPosition updateColumnPosition =
            (TableChange.UpdateColumnPosition) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnPositionFieldDefinition(updateColumnPosition, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnType) {
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateComment) {
        TableChange.UpdateComment updateComment = (TableChange.UpdateComment) change;
        String newComment = updateComment.getNewComment();
        alterSql.add("MODIFY COMMENT \"" + newComment + "\"");
      } else if (change instanceof TableChange.SetProperty) {
        if (hasSetPropertyChange) {
          throw new IllegalArgumentException(
              "StarRocks suggest modify one property at a time, please split it to multiple request.");
        }
        TableChange.SetProperty setProperty = (TableChange.SetProperty) change;
        if (!SUPPORTED_MODIFY_PROPERTIES.contains(setProperty.getProperty())
            && setProperty
                .getProperty()
                .startsWith(SUPPORTED_MODIFY_PROPERTIES_PREFIX_DYNAMIC_PARTITION)
            && setProperty.getProperty().startsWith(SUPPORTED_MODIFY_PROPERTIES_PREFIX_BINLOG)) {
          throw new IllegalArgumentException(
              "Current StarRocks not support modify this table property "
                  + setProperty.getProperty());
        }
        alterSql.add(generateTableProperties(setProperty));
        hasSetPropertyChange = true;
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type : " + change.getClass().getName());
      }
    }

    String result = "ALTER TABLE `" + tableName + "`\n" + String.join(",\n", alterSql) + ";";
    LOG.info("Generated alter table:{}.{} sql: {}", databaseName, tableName, result);
    return result;
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

    return Collections.unmodifiableMap(StarRocksUtils.extractPropertiesFromSql(createTableSql));
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
          StarRocksUtils.extractPartitionInfoFromSql(createTableSql.toString());
      return transform.map(t -> new Transform[] {t}).orElse(Transforms.EMPTY_TRANSFORM);
    }
  }

  @Override
  protected void correctJdbcTableFields(
      Connection connection, String databaseName, String tableName, JdbcTable.Builder tableBuilder)
      throws SQLException {
    String showCreateTableSql = String.format("SHOW CREATE TABLE `%s`", tableName);
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(showCreateTableSql)) {
      StringBuilder createTableSql = new StringBuilder();
      if (result.next()) {
        createTableSql.append(result.getString("Create Table"));
      }
      String tableComment = StarRocksUtils.extractTableCommentFromSql(createTableSql.toString());
      tableBuilder.withComment(tableComment);
    }
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return String.format("ALTER TABLE `%s` RENAME `%s`", oldTableName, newTableName);
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    // never called, as implemented generatePurgeTableSql(String databaseName, String tableName)
    throw new UnsupportedOperationException(
        "StarRocks does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generatePurgeTableSql(String databaseName, String tableName) {
    return String.format("TRUNCATE TABLE `%s`.`%s`", databaseName, tableName);
  }

  @Override
  protected Distribution getDistributionInfo(
      Connection connection, String databaseName, String tableName) throws SQLException {

    String showCreateTableSql = String.format("SHOW CREATE TABLE `%s`", tableName);
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(showCreateTableSql)) {
      result.next();
      String createTableSyntax = result.getString("Create Table");
      return StarRocksUtils.extractDistributionInfoFromSql(createTableSyntax);
    }
  }

  public StringBuilder appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
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
      sqlBuilder.append(AUTO_INCREMENT).append(" ");
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
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
      // We do not support multi-column range partitioning in StarRocks for now
      Transforms.RangeTransform rangePartition = (Transforms.RangeTransform) partitioning[0];
      partitionSqlBuilder = generateRangePartitionSql(rangePartition, columnNames);
    } else if (partitioning[0] instanceof Transforms.ListTransform) {
      Transforms.ListTransform listPartition = (Transforms.ListTransform) partitioning[0];
      partitionSqlBuilder = generateListPartitionSql(listPartition, columnNames);
    } else {
      throw new IllegalArgumentException("Unsupported partition type of StarRocks");
    }

    sqlBuilder.append(partitionSqlBuilder);
  }

  private static StringBuilder generateRangePartitionSql(
      Transforms.RangeTransform rangePartition, Set<String> columnNames) {
    Preconditions.checkArgument(
        rangePartition.fieldName().length == 1,
        "StarRocks partition does not support nested field");
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
              .map(StarRocksUtils::generatePartitionSqlFragment)
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
          filedName.length == 1, "StarRocks partition does not support nested field");
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

        partitions.add(StarRocksUtils.generatePartitionSqlFragment(part));
      }
      partitionSqlBuilder
          .append(NEW_LINE)
          .append(partitions.build().stream().collect(Collectors.joining("," + NEW_LINE)));
    }

    partitionSqlBuilder.append(NEW_LINE).append(")");
    return partitionSqlBuilder;
  }

  private static void addDistributionSql(Distribution distribution, StringBuilder sqlBuilder) {
    if (distribution == null || distribution.strategy() == Strategy.NONE) {
      return;
    }
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
    if (distribution.number() != Distributions.AUTO) {
      sqlBuilder
          .append(" BUCKETS ")
          .append(StarRocksUtils.toBucketNumberString(distribution.number()));
    }
  }

  private static void addPropertiesSql(Map<String, String> properties, StringBuilder sqlBuilder) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    sqlBuilder.append("\n").append(StarRocksUtils.generatePropertiesSql(properties));
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    String dataType = typeConverter.fromGravitino(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("StarRocks does not support nested column names.");
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
      // do nothing
    } else {
      throw new IllegalArgumentException("Invalid column position.");
    }
    return columnDefinition.toString();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable jdbcTable) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("StarRocks does not support nested column names.");
    }
    String col = deleteColumn.fieldName()[0];
    try {
      getJdbcColumnFromTable(jdbcTable, col);
    } catch (NoSuchColumnException ex) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column does not exist: " + col);
      }
    }
    return "DROP COLUMN " + BACK_QUOTE + col + BACK_QUOTE;
  }

  private String renameColumnDefinition(
      TableChange.RenameColumn renameColumn, JdbcTable jdbcTable) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("StarRocks does not support nested column names.");
    }
    String oldColName = renameColumn.fieldName()[0];
    try {
      getJdbcColumnFromTable(jdbcTable, oldColName);
    } catch (NoSuchColumnException ex) {
      throw new IllegalArgumentException("Original column does not exist: " + oldColName);
    }
    try {
      getJdbcColumnFromTable(jdbcTable, renameColumn.getNewName());
    } catch (NoSuchColumnException ex) {
      return String.format("RENAME COLUMN %s TO %s", oldColName, renameColumn.getNewName());
    }
    throw new IllegalArgumentException("Column already exists: " + renameColumn.getNewName());
  }

  private String renameTableDefinition(TableChange.RenameTable renameTable, JdbcTable jdbcTable) {
    try {
      load(jdbcTable.databaseName(), renameTable.getNewName());
    } catch (NoSuchTableException ex) {
      return "RENAME " + renameTable.getNewName();
    }
    throw new IllegalArgumentException("Table already exists: " + renameTable.getNewName());
  }

  private String generateTableProperties(TableChange.SetProperty setProperty) {
    return String.format(
        "set ( \"%s\" = \"%s\" )", setProperty.getProperty(), setProperty.getValue());
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException("StarRocks does not support nested column names.");
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

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException("StarRocks does not support nested column names.");
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
}
