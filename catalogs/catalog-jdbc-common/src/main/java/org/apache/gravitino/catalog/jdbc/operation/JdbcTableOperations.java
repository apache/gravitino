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
package org.apache.gravitino.catalog.jdbc.operation;

import com.google.common.base.Preconditions;
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
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.bean.JdbcIndexBean;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for managing tables in a JDBC data store. */
public abstract class JdbcTableOperations implements TableOperation {

  public static final String COMMENT = "COMMENT";
  public static final String SPACE = " ";
  public static final String TIME_FORMAT_WITH_DOT = "HH:MM:SS.";
  public static final String DATETIME_FORMAT_WITH_DOT = "YYYY-MM-DD HH:MM:SS.";

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
      Distribution distribution,
      Index[] indexes)
      throws TableAlreadyExistsException {
    LOG.info("Attempting to create table {} in database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          generateCreateTableSql(
              tableName, columns, comment, properties, partitioning, distribution, indexes));
      LOG.info("Created table {} in database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public boolean drop(String databaseName, String tableName) {
    LOG.info("Attempting to delete table {} from database {}", tableName, databaseName);
    try {
      dropTable(databaseName, tableName);
      LOG.info("Deleted table {} from database {}", tableName, databaseName);
    } catch (NoSuchTableException e) {
      return false;
    } catch (NoSuchSchemaException e) {
      return false;
    }
    return true;
  }

  /**
   * The default implementation of this method is based on MySQL, and if the catalog does not
   * compatible with MySQL, this method needs to be rewritten.
   */
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

  /**
   * Get table information from the result set and attach it to the table builder, If the table is
   * not found, it will throw a NoSuchTableException.
   *
   * @param tablesResult The result set of the table
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @return The builder of the table to be returned
   * @throws SQLException if a database access error occurs.
   */
  protected JdbcTable.Builder getTableBuilder(
      ResultSet tablesResult, String databaseName, String tableName) throws SQLException {
    boolean found = false;
    JdbcTable.Builder builder = null;
    while (tablesResult.next() && !found) {
      if (Objects.equals(tablesResult.getString("TABLE_NAME"), tableName)) {
        builder = getBasicJdbcTableInfo(tablesResult).withDatabaseName(databaseName);
        found = true;
      }
    }

    if (!found) {
      throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
    }

    return builder;
  }

  protected JdbcColumn.Builder getColumnBuilder(
      ResultSet columnsResult, String databaseName, String tableName) throws SQLException {
    JdbcColumn.Builder builder = null;
    if (Objects.equals(columnsResult.getString("TABLE_NAME"), tableName)) {
      builder = getBasicJdbcColumnInfo(columnsResult);
    }
    return builder;
  }

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    // We should handle case sensitivity and wild card issue in some catalog tables, take MySQL
    // tables, for example.
    // 1. MySQL will get table 'a_b' and 'A_B' when we query 'a_b' in a case-insensitive charset
    // like utf8mb4.
    // 2. MySQL treats 'a_b' as a wildcard, matching any table name that begins with 'a', followed
    // by any character, and ending with 'b'.
    try (Connection connection = getConnection(databaseName)) {
      // 1. Get table information, The result of tables may be more than one due to the reason
      // above, so we need to check the result.
      ResultSet tables = getTable(connection, databaseName, tableName);
      JdbcTable.Builder jdbcTableBuilder = getTableBuilder(tables, databaseName, tableName);

      // 2.Get column information
      List<JdbcColumn> jdbcColumns = new ArrayList<>();
      // Get columns are wildcard sensitive, so we need to check the result.
      ResultSet columns = getColumns(connection, databaseName, tableName);
      while (columns.next()) {
        // TODO(yunqing): check schema and catalog also
        JdbcColumn.Builder columnBuilder = getColumnBuilder(columns, databaseName, tableName);
        if (columnBuilder != null) {
          boolean autoIncrement = getAutoIncrementInfo(columns);
          columnBuilder.withAutoIncrement(autoIncrement);
          jdbcColumns.add(columnBuilder.build());
        }
      }
      jdbcTableBuilder.withColumns(jdbcColumns.toArray(new JdbcColumn[0]));

      // 3.Get index information
      List<Index> indexes = getIndexes(connection, databaseName, tableName);
      jdbcTableBuilder.withIndexes(indexes.toArray(new Index[0]));

      // 4.Get partitioning
      Transform[] tablePartitioning = getTablePartitioning(connection, databaseName, tableName);
      jdbcTableBuilder.withPartitioning(tablePartitioning);

      // 5.Get distribution information
      Distribution distribution = getDistributionInfo(connection, databaseName, tableName);
      jdbcTableBuilder.withDistribution(distribution);

      // 6.Get table properties
      Map<String, String> tableProperties = getTableProperties(connection, tableName);
      jdbcTableBuilder.withProperties(tableProperties);

      // 7.Leave the information to the bottom layer to append the table
      correctJdbcTableFields(connection, databaseName, tableName, jdbcTableBuilder);

      return jdbcTableBuilder.withTableOperation(this).build();
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
   * @throws SQLException if a database access error occurs
   */
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    return Collections.emptyMap();
  }

  protected Transform[] getTablePartitioning(
      Connection connection, String databaseName, String tableName) throws SQLException {
    return Transforms.EMPTY_TRANSFORM;
  }

  /**
   * Get the distribution information of the table, including the distribution type and the fields
   *
   * @param connection jdbc connection.
   * @param databaseName database name.
   * @param tableName table name.
   * @return Returns the distribution information of the table.
   * @throws SQLException if an error occurs while getting the distribution information.
   */
  protected Distribution getDistributionInfo(
      Connection connection, String databaseName, String tableName) throws SQLException {
    return Distributions.NONE;
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
  public boolean purge(String databaseName, String tableName) {
    try {
      purgeTable(databaseName, tableName);
    } catch (NoSuchTableException | NoSuchSchemaException e) {
      return false;
    }
    return true;
  }

  protected void purgeTable(String databaseName, String tableName) {
    LOG.info("Attempting to purge table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(connection, generatePurgeTableSql(databaseName, tableName));
      LOG.info("Purge table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected void dropTable(String databaseName, String tableName) {
    LOG.info("Attempting to delete table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(connection, generateDropTableSql(tableName));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected ResultSet getTables(Connection connection) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String catalogName = connection.getCatalog();
    String schemaName = connection.getSchema();
    return metaData.getTables(catalogName, schemaName, null, JdbcConnectorUtils.getTableTypes());
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
   * @param databaseName The name of the database
   * @param tableName table name
   * @param jdbcTableBuilder The builder of the table to be returned
   * @throws SQLException if a database access error occurs
   */
  protected void correctJdbcTableFields(
      Connection connection,
      String databaseName,
      String tableName,
      JdbcTable.Builder jdbcTableBuilder)
      throws SQLException {
    // nothing to do
  }

  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
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
      String loadedTableName = indexInfo.getString("TABLE_NAME");
      // The primary key is also the unique key, so we need to filter the primary key here.
      if (loadedTableName.equals(tableName)
          && !indexInfo.getBoolean("NON_UNIQUE")
          && !primaryIndexNames.contains(indexName)) {
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
      Distribution distribution,
      Index[] indexes);

  /**
   * The default implementation of this method is based on MySQL syntax, and if the catalog does not
   * support MySQL syntax, this method needs to be rewritten.
   *
   * @param oldTableName The original table name
   * @param newTableName The new table name
   * @return The SQL statement to rename a table
   */
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return String.format("RENAME TABLE `%s` TO `%s`", oldTableName, newTableName);
  }

  /**
   * The default implementation of this method is based on MySQL syntax, and if the catalog does not
   * support MySQL syntax, this method needs to be rewritten.
   *
   * @param tableName The name of the table to be dropped
   * @return The SQL statement to drop a table
   */
  protected String generateDropTableSql(String tableName) {
    return String.format("DROP TABLE `%s`", tableName);
  }

  protected abstract String generatePurgeTableSql(String tableName);

  protected String generatePurgeTableSql(String databaseName, String tableName) {
    return generatePurgeTableSql(tableName);
  }

  protected abstract String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes);

  /**
   * The default implementation of this method is based on MySQL syntax, and if the catalog does not
   * support MySQL syntax, this method needs to be rewritten.
   *
   * @param databaseName The name of the database
   * @param tableName The name of the table
   * @param lazyLoadCreateTable The pre-loaded table object, if available
   * @return The resulting JdbcTable object
   */
  protected JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    return null != lazyLoadCreateTable ? lazyLoadCreateTable : load(databaseName, tableName);
  }

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

  /**
   * The auto-increment column will be verified. There can only be one auto-increment column and it
   * must be the primary key or unique index.
   *
   * @param columns jdbc column
   * @param indexes table indexes
   */
  protected static void validateIncrementCol(JdbcColumn[] columns, Index[] indexes) {
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

  /**
   * The default implementation of this method is based on MySQL syntax, and if the catalog does not
   * support MySQL syntax, this method needs to be rewritten.
   *
   * @param fieldNames The 2D array of index field names
   * @return A comma-separated string of index field names
   */
  protected static String getIndexFieldStr(String[][] fieldNames) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in this Catalog");
              }
              return String.format("`%s`", colNames[0]);
            })
        .collect(Collectors.joining(", "));
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
    return JdbcTable.builder()
        .withName(table.getString("TABLE_NAME"))
        .withComment(table.getString("REMARKS"))
        .withAuditInfo(AuditInfo.EMPTY);
  }

  protected JdbcColumn.Builder getBasicJdbcColumnInfo(ResultSet column) throws SQLException {
    String typeName = column.getString("TYPE_NAME");
    int columnSize = column.getInt("COLUMN_SIZE");
    int scale = column.getInt("DECIMAL_DIGITS");
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean(typeName);
    typeBean.setColumnSize(columnSize);
    typeBean.setScale(scale);
    Integer datetimePrecision = calculateDatetimePrecision(typeName, columnSize, scale);
    typeBean.setDatetimePrecision(datetimePrecision);

    String comment = column.getString("REMARKS");
    boolean nullable = column.getBoolean("NULLABLE");

    String columnDef = column.getString("COLUMN_DEF");
    boolean isExpression = "YES".equals(column.getString("IS_GENERATEDCOLUMN"));
    Expression defaultValue =
        columnDefaultValueConverter.toGravitino(typeBean, columnDef, isExpression, nullable);

    return JdbcColumn.builder()
        .withName(column.getString("COLUMN_NAME"))
        .withType(typeConverter.toGravitino(typeBean))
        .withComment(StringUtils.isEmpty(comment) ? null : comment)
        .withNullable(nullable)
        .withDefaultValue(defaultValue);
  }

  /**
   * Calculate the precision for time/datetime/timestamp types.
   *
   * <p>This method provides a default behavior returning null for catalogs that do not implement
   * this method. Developers should override this method when their database supports precision
   * specification for datetime types.
   *
   * <p><strong>When to return null:</strong>
   *
   * <ul>
   *   <li>When the database does not support precision for datetime types
   *   <li>When the driver version is incompatible (e.g., MySQL driver &lt; 8.0.16)
   *   <li>When the type is not a datetime type (TIME, TIMESTAMP, DATETIME)
   *   <li>When the precision cannot be accurately calculated from the provided parameters
   * </ul>
   *
   * <p><strong>When to return non-null:</strong>
   *
   * <ul>
   *   <li>When the database supports precision for datetime types and the driver version is
   *       compatible
   *   <li>When the precision can be accurately calculated from columnSize (e.g., columnSize -
   *       format_length)
   *   <li>For TIME types: return columnSize - 8 (for 'HH:MM:SS' format)
   *   <li>For TIMESTAMP/DATETIME types: return columnSize - 19 (for 'YYYY-MM-DD HH:MM:SS' format)
   * </ul>
   *
   * <p><strong>Examples:</strong>
   *
   * <ul>
   *   <li>TIME(3) with columnSize=11: return 3 (11-8)
   *   <li>TIMESTAMP(6) with columnSize=25: return 6 (25-19)
   *   <li>DATETIME(0) with columnSize=19: return 0 (19-19)
   * </ul>
   *
   * @param typeName the type name from database (e.g., "TIME", "TIMESTAMP", "DATETIME")
   * @param columnSize the column size from database (total length including format and precision)
   * @param scale the scale from database (usually 0 for datetime types)
   * @return the precision of the time/datetime/timestamp type, or null if not supported/calculable
   */
  public Integer calculateDatetimePrecision(String typeName, int columnSize, int scale) {
    return null;
  }

  /**
   * Get MySQL driver version from DatabaseMetaData
   *
   * @return the driver version string, or null if not available
   */
  protected String getMySQLDriverVersion() {
    try {
      if (dataSource != null) {
        try (Connection connection = dataSource.getConnection()) {
          return connection.getMetaData().getDriverVersion();
        }
      }
    } catch (SQLException e) {
      LOG.debug("Failed to get driver version", e);
    }
    return null;
  }

  /**
   * Check if driver version supports accurate columnSize for precision calculation. For MySQL
   * driver: Only versions &gt;= 8.0.16 return accurate columnSize for datetime precision. For other
   * drivers (like OceanBase): Assume they support accurate precision calculation
   *
   * <p>MySQL Connector/J 8.0.16 fixed the wrong handling of temporal type precision in the
   * DatabaseMetaDataUsingInfoSchema interface, where getColumns() method returned wrong results for
   * the COLUMN_SIZE column. Prior versions had errors in temporal type precision handling.
   *
   * @param driverVersion the driver version string to check
   * @return true if the driver version supports accurate precision calculation, false otherwise
   * @see <a href="https://dev.mysql.com/doc/relnotes/connector-j/en/news-8-0-16.html">MySQL
   *     Connector/J 8.0.16 Release Notes</a>
   */
  public boolean isMySQLDriverVersionSupported(String driverVersion) {
    if (StringUtils.isBlank(driverVersion)) {
      return false;
    }

    // For non-MySQL drivers, assume they support accurate precision calculation
    if (!driverVersion.startsWith("mysql-connector-java-")) {
      LOG.debug(
          "Non-MySQL driver detected: {}. Assuming it supports accurate precision calculation.",
          driverVersion);
      return true;
    }

    // Extract version from driver string like "mysql-connector-java-8.0.19 (Revision: ...)"
    String versionPattern = "mysql-connector-java-(\\d+\\.\\d+\\.\\d+)";
    Pattern pattern = Pattern.compile(versionPattern);
    Matcher matcher = pattern.matcher(driverVersion);

    if (matcher.find()) {
      String versionStr = matcher.group(1);
      try {
        String[] parts = versionStr.split("\\.");
        int major = Integer.parseInt(parts[0]);
        int minor = Integer.parseInt(parts[1]);
        int patch = Integer.parseInt(parts[2]);

        // Check if version >= 8.0.16
        if (major > 8) {
          return true;
        } else if (major == 8 && minor > 0) {
          return true;
        } else {
          return major == 8 && minor == 0 && patch >= 16;
        }
      } catch (NumberFormatException e) {
        LOG.debug("Failed to parse driver version: {}", versionStr, e);
        return false;
      }
    }

    return false;
  }
}
