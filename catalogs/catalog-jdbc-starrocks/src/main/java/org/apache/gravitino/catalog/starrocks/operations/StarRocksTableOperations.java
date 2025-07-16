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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.catalog.starrocks.utils.StarRocksUtils;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

/** Table operations for StarRocks. */
public class StarRocksTableOperations extends JdbcTableOperations {

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
    throw new NotImplementedException("To be implemented in the future");
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
    if (StringUtils.isNotEmpty(tableBuilder.comment())) {
      return;
    }

    // StarRocks Cannot get comment from JDBC 8.x, so we need to get comments from SQL.
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
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "StarRocks does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw new NotImplementedException("To be implemented in the future");
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
}
