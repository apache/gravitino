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
package org.apache.gravitino.catalog.generic.operation;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.generic.GenericJdbcMetadataConfig;
import org.apache.gravitino.catalog.generic.converter.GenericJdbcTypeConverter.GenericJdbcTypeBean;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/** Read-only table operations for the generic JDBC catalog. */
public class GenericJdbcTableOperations extends JdbcTableOperations
    implements RequireDatabaseOperation {

  private static final String TABLE_NAME = "TABLE_NAME";
  private static final String TABLE_SCHEM = "TABLE_SCHEM";

  private GenericJdbcMetadataConfig metadataConfig;
  private GenericJdbcDatabaseOperations databaseOperations;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf) {
    super.initialize(
        dataSource, exceptionMapper, jdbcTypeConverter, jdbcColumnDefaultValueConverter, conf);
    this.metadataConfig = new GenericJdbcMetadataConfig(conf);
  }

  @Override
  public void setDatabaseOperation(DatabaseOperation databaseOperation) {
    this.databaseOperations = (GenericJdbcDatabaseOperations) databaseOperation;
  }

  @Override
  public void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes)
      throws TableAlreadyExistsException {
    throw GenericJdbcDatabaseOperations.readOnlyException("create table");
  }

  @Override
  public void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes,
      SortOrder[] sortOrders)
      throws TableAlreadyExistsException {
    throw GenericJdbcDatabaseOperations.readOnlyException("create table");
  }

  @Override
  public boolean drop(String databaseName, String tableName) {
    throw GenericJdbcDatabaseOperations.readOnlyException("drop table");
  }

  @Override
  public void rename(String databaseName, String oldTableName, String newTableName)
      throws NoSuchTableException {
    throw GenericJdbcDatabaseOperations.readOnlyException("rename table");
  }

  @Override
  public void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException {
    throw GenericJdbcDatabaseOperations.readOnlyException("alter table");
  }

  @Override
  public boolean purge(String databaseName, String tableName) {
    throw GenericJdbcDatabaseOperations.readOnlyException("purge table");
  }

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    checkSchemaExists(databaseName);
    List<String> tableNames = new ArrayList<>();
    try (Connection connection = getConnection(databaseName);
        ResultSet tables = getTables(connection, databaseName)) {
      while (tables.next()) {
        String tableName = tables.getString(TABLE_NAME);
        if (tableName != null && Objects.equals(tables.getString(TABLE_SCHEM), databaseName)) {
          tableNames.add(tableName);
        }
      }
      return tableNames;
    } catch (SQLException se) {
      throw exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected Connection getConnection(String catalog) throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  protected ResultSet getTable(Connection connection, String databaseName, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(
        metadataConfig.catalogPattern(), databaseName, tableName, metadataConfig.tableTypes());
  }

  @Override
  protected ResultSet getColumns(Connection connection, String databaseName, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(metadataConfig.catalogPattern(), databaseName, tableName, null);
  }

  @Override
  protected ResultSet getIndexInfo(String databaseName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(
        metadataConfig.catalogPattern(), databaseName, tableName, false, false);
  }

  @Override
  protected ResultSet getPrimaryKeys(
      String databaseName, String tableName, DatabaseMetaData metaData) throws SQLException {
    return metaData.getPrimaryKeys(metadataConfig.catalogPattern(), databaseName, tableName);
  }

  @Override
  protected JdbcTable.Builder getTableBuilder(
      ResultSet tablesResult, String databaseName, String tableName) throws SQLException {
    while (tablesResult.next()) {
      if (Objects.equals(tablesResult.getString(TABLE_NAME), tableName)
          && Objects.equals(tablesResult.getString(TABLE_SCHEM), databaseName)) {
        return getBasicJdbcTableInfo(tablesResult).withDatabaseName(databaseName);
      }
    }

    throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
  }

  @Override
  protected JdbcColumn.Builder getColumnBuilder(
      ResultSet columnsResult, String databaseName, String tableName) throws SQLException {
    if (Objects.equals(columnsResult.getString(TABLE_NAME), tableName)
        && Objects.equals(columnsResult.getString(TABLE_SCHEM), databaseName)) {
      return getBasicJdbcColumnInfo(columnsResult);
    }
    return null;
  }

  @Override
  protected JdbcColumn.Builder getBasicJdbcColumnInfo(ResultSet column) throws SQLException {
    String typeName = column.getString("TYPE_NAME");
    int columnSize = column.getInt("COLUMN_SIZE");
    boolean columnSizeWasNull = column.wasNull();
    int scale = column.getInt("DECIMAL_DIGITS");
    boolean scaleWasNull = column.wasNull();
    GenericJdbcTypeBean typeBean = new GenericJdbcTypeBean(typeName, column.getInt("DATA_TYPE"));
    typeBean.setColumnSize(columnSizeWasNull ? null : columnSize);
    typeBean.setScale(scaleWasNull ? null : scale);

    String comment = column.getString("REMARKS");
    boolean nullable = column.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
    String columnDef = column.getString("COLUMN_DEF");
    Expression defaultValue =
        columnDefaultValueConverter.toGravitino(typeBean, columnDef, false, nullable);

    return JdbcColumn.builder()
        .withName(column.getString("COLUMN_NAME"))
        .withType(typeConverter.toGravitino(typeBean))
        .withComment(comment == null || comment.isEmpty() ? null : comment)
        .withNullable(nullable)
        .withDefaultValue(defaultValue);
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
    throw GenericJdbcDatabaseOperations.readOnlyException("create table");
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw GenericJdbcDatabaseOperations.readOnlyException("purge table");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw GenericJdbcDatabaseOperations.readOnlyException("alter table");
  }

  private ResultSet getTables(Connection connection, String databaseName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(
        metadataConfig.catalogPattern(), databaseName, null, metadataConfig.tableTypes());
  }

  private void checkSchemaExists(String databaseName) {
    if (databaseOperations != null && !databaseOperations.exist(databaseName)) {
      throw new NoSuchSchemaException("No such schema: %s", databaseName);
    }
  }
}
