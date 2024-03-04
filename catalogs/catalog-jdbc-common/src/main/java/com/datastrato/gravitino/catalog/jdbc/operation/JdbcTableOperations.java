/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.bean.JdbcIndexBean;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for managing tables in a JDBC data store. */
public abstract class JdbcTableOperations implements TableOperation {

  public static final String COMMENT = "COMMENT";
  public static final String SPACE = " ";

  public static final String MODIFY_COLUMN = "MODIFY COLUMN ";
  public static final String AFTER = "AFTER ";

  protected static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);

  protected DataSource dataSource;
  protected JdbcExceptionConverter exceptionMapper;
  protected JdbcTypeConverter typeConverter;

  protected JdbcColumnDefaultValueConverter columnDefaultValueConverter;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf) {
    this.dataSource = dataSource;
    this.exceptionMapper = exceptionMapper;
    this.typeConverter = jdbcTypeConverter;
    this.columnDefaultValueConverter = jdbcColumnDefaultValueConverter;
  }

  @Override
  public void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes)
      throws TableAlreadyExistsException {
    LOG.info("Attempting to create table {} in database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          generateCreateTableSql(tableName, columns, comment, properties, partitioning, indexes));
      LOG.info("Created table {} in database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public void drop(String databaseName, String tableName) throws NoSuchTableException {
    LOG.info("Attempting to delete table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(connection, generateDropTableSql(tableName));
      LOG.info("Deleted table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(databaseName)) {
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        while (tables.next()) {
          if (Objects.equals(tables.getString("TABLE_SCHEM"), databaseName)) {
            names.add(tables.getString("TABLE_NAME"));
          }
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), databaseName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    // We should handle case sensitivity and wild card issue in some catalog tables, take a MySQL
    // table for example.
    // 1. MySQL will get table 'a_b' and 'A_B' when we query 'a_b' in a case-insensitive charset
    // like utf8mb4.
    // 2. MySQL treats 'a_b' as a wildcard, matching any table name that begins with 'a', followed
    // by any character, and ending with 'b'.
    try (Connection connection = getConnection(databaseName)) {
      // 1.Get table information
      ResultSet table = getTable(connection, databaseName, tableName);
      // The result of tables may be more than one due to the reason above, so we need to check the
      // result
      JdbcTable.Builder jdbcTableBuilder = new JdbcTable.Builder();
      boolean found = false;
      // Handle case-sensitive issues.
      while (table.next() && !found) {
        if (Objects.equals(table.getString("TABLE_NAME"), tableName)) {
          jdbcTableBuilder = getBasicJdbcTableInfo(table);
          found = true;
        }
      }

      if (!found) {
        throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
      }

      // 2.Get column information
      List<JdbcColumn> jdbcColumns = new ArrayList<>();
      // Get columns are wildcard sensitive, so we need to check the result.
      ResultSet columns = getColumns(connection, databaseName, tableName);
      while (columns.next()) {
        // TODO(yunqing): check schema and catalog also
        if (Objects.equals(columns.getString("TABLE_NAME"), tableName)) {
          JdbcColumn.Builder columnBuilder = getBasicJdbcColumnInfo(columns);
          boolean autoIncrement = getAutoIncrementInfo(columns);
          columnBuilder.withAutoIncrement(autoIncrement);
          jdbcColumns.add(columnBuilder.build());
        }
      }
      jdbcTableBuilder.withColumns(jdbcColumns.toArray(new JdbcColumn[0]));

      // 3.Get index information
      List<Index> indexes = getIndexes(databaseName, tableName, connection.getMetaData());
      jdbcTableBuilder.withIndexes(indexes.toArray(new Index[0]));

      // 4.Get table properties
      Map<String, String> tableProperties = getTableProperties(connection, tableName);
      jdbcTableBuilder.withProperties(tableProperties);

      // 5.Leave the information to the bottom layer to append the table
      correctJdbcTableFields(connection, tableName, jdbcTableBuilder);
      return jdbcTableBuilder.build();
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  /**
   * Get all properties values of the table, including properties outside Gravitino management. The
   * JdbcCatalogOperations#loadTable method will filter out unnecessary properties.
   *
   * @param connection jdbc connection
   * @param tableName table name
   * @return Returns all table properties values.
   * @throws SQLException
   */
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    return Collections.emptyMap();
  }

  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return resultSet.getBoolean("IS_AUTOINCREMENT");
  }

  @Override
  public void rename(String databaseName, String oldTableName, String newTableName)
      throws NoSuchTableException {
    LOG.info(
        "Attempting to rename table {}/{} to {}/{}",
        databaseName,
        oldTableName,
        databaseName,
        newTableName);
    try (Connection connection = this.getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection, this.generateRenameTableSql(oldTableName, newTableName));
      LOG.info(
          "Renamed table {}/{} to {}/{}", databaseName, oldTableName, databaseName, newTableName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
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
  public void purge(String databaseName, String tableName) throws NoSuchTableException {
    LOG.info("Attempting to purge table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(connection, generatePurgeTableSql(tableName));
      LOG.info("Purge table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected ResultSet getTables(Connection connection) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String databaseName = connection.getSchema();
    return metaData.getTables(databaseName, databaseName, null, JdbcConnectorUtils.getTableTypes());
  }

  protected ResultSet getTable(Connection connection, String databaseName, String tableName)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(connection.getCatalog(), connection.getSchema(), tableName, null);
  }

  protected ResultSet getColumns(Connection connection, String databaseName, String tableName)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, null);
  }

  /**
   * Correct table information from the JDBC driver, because not all information could be retrieved
   * from the JDBC driver, like the table comment in MySQL of the 5.7 version.
   *
   * @param connection jdbc connection
   * @param tableName table name
   * @param jdbcTableBuilder The builder of the table to be returned
   * @throws SQLException
   */
  protected void correctJdbcTableFields(
      Connection connection, String tableName, JdbcTable.Builder jdbcTableBuilder)
      throws SQLException {
    // nothing to do
  }

  protected List<Index> getIndexes(String databaseName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    List<Index> indexes = new ArrayList<>();

    // Get primary key information
    ResultSet primaryKeys = getPrimaryKeys(databaseName, tableName, metaData);
    List<JdbcIndexBean> jdbcIndexBeans = new ArrayList<>();
    while (primaryKeys.next()) {
      jdbcIndexBeans.add(
          new JdbcIndexBean(
              Index.IndexType.PRIMARY_KEY,
              primaryKeys.getString("COLUMN_NAME"),
              primaryKeys.getString("PK_NAME"),
              primaryKeys.getInt("KEY_SEQ")));
    }

    Set<String> primaryIndexNames =
        jdbcIndexBeans.stream().map(JdbcIndexBean::getName).collect(Collectors.toSet());

    // Get unique key information
    ResultSet indexInfo = getIndexInfo(databaseName, tableName, metaData);
    while (indexInfo.next()) {
      String indexName = indexInfo.getString("INDEX_NAME");
      // The primary key is also the unique key, so we need to filter the primary key here.
      if (!indexInfo.getBoolean("NON_UNIQUE") && !primaryIndexNames.contains(indexName)) {
        jdbcIndexBeans.add(
            new JdbcIndexBean(
                Index.IndexType.UNIQUE_KEY,
                indexInfo.getString("COLUMN_NAME"),
                indexName,
                indexInfo.getInt("ORDINAL_POSITION")));
      }
    }

    // Assemble into Index
    Map<Index.IndexType, List<JdbcIndexBean>> indexBeanGroupByIndexType =
        jdbcIndexBeans.stream().collect(Collectors.groupingBy(JdbcIndexBean::getIndexType));

    for (Map.Entry<Index.IndexType, List<JdbcIndexBean>> entry :
        indexBeanGroupByIndexType.entrySet()) {
      // Group by index Name
      Map<String, List<JdbcIndexBean>> indexBeanGroupByName =
          entry.getValue().stream().collect(Collectors.groupingBy(JdbcIndexBean::getName));
      for (Map.Entry<String, List<JdbcIndexBean>> indexEntry : indexBeanGroupByName.entrySet()) {
        List<String> colNames =
            indexEntry.getValue().stream()
                .sorted(Comparator.comparingInt(JdbcIndexBean::getOrder))
                .map(JdbcIndexBean::getColName)
                .collect(Collectors.toList());
        String[][] colStrArrays = convertIndexFieldNames(colNames);
        if (entry.getKey() == Index.IndexType.PRIMARY_KEY) {
          indexes.add(Indexes.primary(indexEntry.getKey(), colStrArrays));
        } else {
          indexes.add(Indexes.unique(indexEntry.getKey(), colStrArrays));
        }
      }
    }
    return indexes;
  }

  protected ResultSet getIndexInfo(String databaseName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(databaseName, null, tableName, false, false);
  }

  protected ResultSet getPrimaryKeys(
      String databaseName, String tableName, DatabaseMetaData metaData) throws SQLException {
    return metaData.getPrimaryKeys(databaseName, null, tableName);
  }

  protected String[][] convertIndexFieldNames(List<String> fieldNames) {
    return fieldNames.stream().map(colName -> new String[] {colName}).toArray(String[][]::new);
  }

  protected abstract String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes);

  protected abstract String generateRenameTableSql(String oldTableName, String newTableName);

  protected abstract String generateDropTableSql(String tableName);

  protected abstract String generatePurgeTableSql(String tableName);

  protected abstract String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes);

  protected abstract JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable);

  protected void validateUpdateColumnNullable(
      TableChange.UpdateColumnNullability change, JdbcTable table) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException("Nested column names are not supported");
    }
    String col = change.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(table, col);
    if (!change.nullable() && column.defaultValue().equals(Literals.NULL)) {
      throw new IllegalArgumentException(
          "column " + col + " with null default value cannot be changed to not null");
    }
  }

  protected JdbcColumn getJdbcColumnFromTable(JdbcTable jdbcTable, String colName) {
    return (JdbcColumn)
        Arrays.stream(jdbcTable.columns())
            .filter(column -> column.name().equals(colName))
            .findFirst()
            .orElseThrow(
                () ->
                    new NoSuchColumnException(
                        "Column %s does not exist in table %s", colName, jdbcTable.name()));
  }

  protected Connection getConnection(String catalog) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(catalog);
    return connection;
  }

  protected JdbcTable.Builder getBasicJdbcTableInfo(ResultSet table) throws SQLException {
    return new JdbcTable.Builder()
        .withName(table.getString("TABLE_NAME"))
        .withComment(table.getString("REMARKS"))
        .withAuditInfo(AuditInfo.EMPTY);
  }

  protected JdbcColumn.Builder getBasicJdbcColumnInfo(ResultSet column) throws SQLException {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        new JdbcTypeConverter.JdbcTypeBean(column.getString("TYPE_NAME"));
    typeBean.setColumnSize(column.getString("COLUMN_SIZE"));
    typeBean.setScale(column.getString("DECIMAL_DIGITS"));
    String comment = column.getString("REMARKS");
    boolean nullable = column.getBoolean("NULLABLE");

    String columnDef = column.getString("COLUMN_DEF");
    boolean isExpression = "YES".equals(column.getString("IS_GENERATEDCOLUMN"));
    Expression defaultValue =
        columnDefaultValueConverter.toGravitino(typeBean, columnDef, isExpression, nullable);

    return new JdbcColumn.Builder()
        .withName(column.getString("COLUMN_NAME"))
        .withType(typeConverter.toGravitinoType(typeBean))
        .withComment(StringUtils.isEmpty(comment) ? null : comment)
        .withNullable(nullable)
        .withDefaultValue(defaultValue);
  }
}
