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
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.SETTINGS_PREFIX;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE_PROPERTY_ENTRY;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

public class ClickHouseTableOperations extends JdbcTableOperations {

  @SuppressWarnings("unused")
  private static final String CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Clickhouse does not support nested column names.";

  private static final String QUERY_INDEXES_SQL =
      """
      SELECT NULL AS TABLE_CAT,
             system.tables.database AS TABLE_SCHEM,
             system.tables.name AS TABLE_NAME,
             trim(c.1) AS COLUMN_NAME,
             c.2 AS KEY_SEQ,
             'PRIMARY' AS PK_NAME
      FROM system.tables
      ARRAY JOIN arrayZip(splitByChar(',', primary_key), arrayEnumerate(splitByChar(',', primary_key))) as c
      WHERE system.tables.primary_key <> ''
        AND system.tables.database = '%s'
        AND system.tables.name = '%s'
      ORDER BY COLUMN_NAME
      """;

  @Override
  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName) {
    // cause clickhouse not impl getPrimaryKeys yet, ref:
    // https://github.com/ClickHouse/clickhouse-java/issues/1625
    String sql =
        QUERY_INDEXES_SQL.formatted(quoteIdentifier(databaseName), quoteIdentifier(tableName));
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      List<Index> indexes = new ArrayList<>();
      while (resultSet.next()) {
        String indexName = resultSet.getString("PK_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        indexes.add(
            Indexes.of(Index.IndexType.PRIMARY_KEY, indexName, new String[][] {{columnName}}));
      }
      return indexes;
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
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
      Index[] indexes) {
    throw new UnsupportedOperationException(
        "generateCreateTableSql with out sortOrders in clickhouse is not supported");
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes,
      SortOrder[] sortOrders) {

    // These two are not yet supported in Gravitino now and will be supported in the future.
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in clickhouse");
    }
    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution), "ClickHouse does not support distribution");

    // First build the CREATE TABLE statement
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE %s (\n".formatted(quoteIdentifier(tableName)));

    // Add columns
    buildColumnsDefinition(columns, sqlBuilder);

    // Index definition, we only support primary index now, secondary index will be supported in
    // future
    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append("\n)");

    // Extract engine from properties
    ClickHouseTablePropertiesMetadata.ENGINE engine = appendTableEngine(properties, sqlBuilder);

    appendOrderBy(sortOrders, sqlBuilder, engine);

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      String escapedComment = comment.replace("'", "''");
      sqlBuilder.append(" COMMENT '%s'".formatted(escapedComment));
    }

    // Add setting clause if specified, clickhouse only supports predefine settings
    appendTableProperties(properties, sqlBuilder);

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  private static void appendTableProperties(
      Map<String, String> properties, StringBuilder sqlBuilder) {
    if (MapUtils.isEmpty(properties)) {
      return;
    }

    String settings =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(SETTINGS_PREFIX))
            .map(
                entry ->
                    entry.getKey().substring(SETTINGS_PREFIX.length()) + " = " + entry.getValue())
            .collect(Collectors.joining(",\n ", " \n SETTINGS ", ""));
    sqlBuilder.append(settings);
  }

  private static void appendOrderBy(
      SortOrder[] sortOrders,
      StringBuilder sqlBuilder,
      ClickHouseTablePropertiesMetadata.ENGINE engine) {
    // ClickHouse requires ORDER BY clause for some engines, and currently only mergeTree family
    // requires ORDER BY clause.
    boolean requireOrderBy = engine.isRequireOrderBy();
    if (!requireOrderBy) {
      if (ArrayUtils.isNotEmpty(sortOrders)) {
        throw new UnsupportedOperationException(
            "ORDER BY clause is not supported for engine: " + engine.getValue());
      }

      // No need to add order by clause
      return;
    }

    if (ArrayUtils.isEmpty(sortOrders)) {
      throw new IllegalArgumentException(
          "ORDER BY clause is required for engine: " + engine.getValue());
    }

    if (sortOrders.length > 1) {
      throw new UnsupportedOperationException(
          "Currently ClickHouse does not support sortOrders with more than 1 element");
    }

    NullOrdering nullOrdering = sortOrders[0].nullOrdering();
    SortDirection sortDirection = sortOrders[0].direction();
    if (nullOrdering != null && sortDirection != null) {
      // ClickHouse does not support NULLS FIRST/LAST now.
      LOG.warn(
          "ClickHouse currently does not support nullOrdering: {}, and will ignore it",
          nullOrdering);
    }

    sqlBuilder.append("\n ORDER BY `%s`\n".formatted(sortOrders[0].expression()));
  }

  private ClickHouseTablePropertiesMetadata.ENGINE appendTableEngine(
      Map<String, String> properties, StringBuilder sqlBuilder) {
    ClickHouseTablePropertiesMetadata.ENGINE engine = ENGINE_PROPERTY_ENTRY.getDefaultValue();
    if (MapUtils.isNotEmpty(properties)) {
      String userSetEngine = properties.remove(CLICKHOUSE_ENGINE_KEY);
      if (StringUtils.isNotEmpty(userSetEngine)) {
        engine = ClickHouseTablePropertiesMetadata.ENGINE.fromString(userSetEngine);
      }
    }
    sqlBuilder.append("\n ENGINE = %s".formatted(engine.getValue()));
    return engine;
  }

  private void buildColumnsDefinition(JdbcColumn[] columns, StringBuilder sqlBuilder) {
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("  %s".formatted(quoteIdentifier(column.name())));

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",\n");
      }
    }
  }

  // ClickHouse only supports primary key now and some secondary index will be supported in future
  private void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    if (ArrayUtils.isEmpty(indexes)) {
      return;
    }

    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          if (null != index.name()
              && !StringUtils.equalsIgnoreCase(index.name(), Indexes.DEFAULT_PRIMARY_KEY_NAME)) {
            LOG.warn(
                "Primary key name must be PRIMARY in ClickHouse, the name {} will be ignored.",
                index.name());
          }
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException(
              "Gravitino Clickhouse doesn't support index : " + index.type());
      }
    }
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT"));
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement("select * from system.tables where name = ? ")) {
      statement.setString(1, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          String name = resultSet.getString("name");
          if (Objects.equals(name, tableName)) {
            return Collections.unmodifiableMap(
                new HashMap<String, String>() {
                  {
                    put(COMMENT, resultSet.getString(COMMENT));
                    put(CLICKHOUSE_ENGINE_KEY, resultSet.getString(CLICKHOUSE_ENGINE_KEY));
                  }
                });
          }
        }

        throw new NoSuchTableException(
            "Table %s does not exist in %s.", tableName, connection.getCatalog());
      }
    }
  }

  protected ResultSet getTables(Connection connection) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    String catalogName = connection.getCatalog();
    String schemaName = connection.getSchema();
    // CK tables include : DICTIONARY", "LOG TABLE", "MEMORY TABLE",
    // "REMOTE TABLE", "TABLE", "VIEW", "SYSTEM TABLE", "TEMPORARY TABLE
    return metaData.getTables(catalogName, schemaName, null, new String[] {"TABLE"});
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "ClickHouse does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw new UnsupportedOperationException(
        "ClickHouseTableOperations.generateAlterTableSql is not implemented yet.");
  }

  private StringBuilder appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add Nullable data type
    String dataType = typeConverter.fromGravitino(column.dataType());
    if (column.nullable()) {
      sqlBuilder.append(" Nullable(%s) ".formatted(dataType));
    } else {
      sqlBuilder.append(" %s ".formatted(dataType));
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder.append(
          " DEFAULT %s "
              .formatted(columnDefaultValueConverter.fromGravitino(column.defaultValue())));
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      String escapedComment = StringUtils.replace(column.comment(), "'", "''");
      sqlBuilder.append("COMMENT '%s' ".formatted(escapedComment));
    }

    return sqlBuilder;
  }
}
