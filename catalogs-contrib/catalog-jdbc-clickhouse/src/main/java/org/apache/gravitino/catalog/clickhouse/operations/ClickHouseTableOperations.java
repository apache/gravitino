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
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

public class ClickHouseTableOperations extends JdbcTableOperations {

  private static final String BACK_QUOTE = "`";

  @SuppressWarnings("unused")
  private static final String CLICKHOUSE_AUTO_INCREMENT = "AUTO_INCREMENT";

  @SuppressWarnings("unused")
  private static final String CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Clickhouse does not support nested column names.";

  @Override
  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName) {
    // cause clickhouse not impl getPrimaryKeys yet, ref:
    // https://github.com/ClickHouse/clickhouse-java/issues/1625
    String sql =
        "SELECT NULL AS TABLE_CAT, "
            + "system.tables.database AS TABLE_SCHEM, "
            + "system.tables.name AS TABLE_NAME, "
            + "trim(c.1) AS COLUMN_NAME, "
            + "c.2 AS KEY_SEQ, "
            + "'PRIMARY' AS PK_NAME "
            + "FROM system.tables "
            + "ARRAY JOIN arrayZip(splitByChar(',', primary_key), arrayEnumerate(splitByChar(',', primary_key))) as c "
            + "WHERE system.tables.primary_key <> '' "
            + "AND system.tables.database = '"
            + databaseName
            + "' "
            + "AND system.tables.name = '"
            + tableName
            + "' "
            + "ORDER BY COLUMN_NAME";

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

    // This two is not yet supported in Gravitino now and will be supported in future.
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in clickhouse");
    }
    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution), "ClickHouse does not support distribution");

    validateIncrementCol(columns, indexes);

    // First build the CREATE TABLE statement
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(BACK_QUOTE)
        .append(tableName)
        .append(BACK_QUOTE)
        .append(" (\n");

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

    // Index definition, we only support primary index now, secondary index will be supported in
    // future
    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append("\n)");

    // Extract engine from properties
    String engine = ENGINE_PROPERTY_ENTRY.getDefaultValue().getValue();
    if (MapUtils.isNotEmpty(properties)) {
      String userSetEngine = properties.remove(CLICKHOUSE_ENGINE_KEY);
      if (StringUtils.isNotEmpty(userSetEngine)) {
        engine = userSetEngine;
      }
    }
    sqlBuilder.append("\n ENGINE = ").append(engine);

    // Omit partition by clause as it will be supported in the next PR
    // TODO (yuqi)

    // Add order by clause
    if (ArrayUtils.isNotEmpty(sortOrders)) {
      if (sortOrders.length > 1) {
        throw new UnsupportedOperationException(
            "Currently we do not support sortOrders's length > 1");
      } else if (sortOrders[0].nullOrdering() != null || sortOrders[0].direction() != null) {
        // If no value is set earlier, some default values will be set.
        // It is difficult to determine whether the user has set a value.
        LOG.warn(
            "clickhouse currently do not support nullOrdering: {} and direction: {} of sortOrders,and will ignore these",
            sortOrders[0].nullOrdering(),
            sortOrders[0].direction());
      }
      sqlBuilder.append(
          " \n ORDER BY " + BACK_QUOTE + sortOrders[0].expression() + BACK_QUOTE + " \n");
    }

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder.append(" COMMENT '").append(comment).append("'");
    }

    // Add setting clause if specified, clickhouse only supports predefine settings
    if (MapUtils.isNotEmpty(properties)) {
      String settings =
          properties.entrySet().stream()
              .filter(entry -> entry.getKey().startsWith(SETTINGS_PREFIX))
              .map(
                  entry ->
                      entry.getKey().substring(SETTINGS_PREFIX.length()) + " = " + entry.getValue())
              .collect(Collectors.joining(",\n ", " \n SETTINGS ", ""));
      sqlBuilder.append(settings);
    }

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  public static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    if (indexes == null) {
      return;
    }

    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          if (null != index.name()
              && !StringUtils.equalsIgnoreCase(
                  index.name(), Indexes.DEFAULT_CLICKHOUSE_PRIMARY_KEY_NAME)) {
            LOG.warn(
                "Primary key name must be PRIMARY in ClickHouse, the name {} will be ignored.",
                index.name());
          }
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          throw new IllegalArgumentException(
              "Gravitino clickHouse doesn't support index : " + index.type());
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
    return metaData.getTables(catalogName, schemaName, null, null);
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
      sqlBuilder.append(SPACE).append("Nullable(").append(dataType).append(")").append(SPACE);
    } else {
      sqlBuilder.append(SPACE).append(dataType).append(SPACE);
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }

    // Add column comment if specified
    if (StringUtils.isNotEmpty(column.comment())) {
      sqlBuilder.append("COMMENT '").append(column.comment()).append("' ");
    }
    return sqlBuilder;
  }
}
