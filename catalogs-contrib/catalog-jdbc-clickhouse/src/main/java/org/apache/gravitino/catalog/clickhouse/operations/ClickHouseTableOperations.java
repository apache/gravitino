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

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.BLOOM_FILTER_SIZE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DATA_SKIPPING_BLOOM_FILTER;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DATA_SKIPPING_MINMAX_VALUE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DATA_SKIPPING_NGRAMBFV1;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DATA_SKIPPING_SET;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DATA_SKIPPING_TOKENBFV1;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.GRANULARITY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.HASH_FUNCTIONS;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.NGRAM_SIZE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.RANDOM_SEED;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.SET_MAX_VALUES;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE_PROPERTY_ENTRY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.GRAVITINO_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.escapeSingleQuotes;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.TableConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

public class ClickHouseTableOperations extends JdbcTableOperations {

  private static final String CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "Clickhouse does not support nested column names.";
  /** Default GRANULARITY for data skipping indexes, matching ClickHouse's own default. */
  private static final long DEFAULT_INDEX_GRANULARITY = 1;

  private static final BigInteger MIN_SET_MAX_VALUES = BigInteger.ZERO;
  private static final BigInteger MAX_SET_MAX_VALUES = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final String SET_MAX_VALUES_RANGE =
      "[%s, %s]".formatted(MIN_SET_MAX_VALUES, MAX_SET_MAX_VALUES);
  private static final Pattern SET_MAX_VALUES_PATTERN = Pattern.compile("[+-]?[0-9]+");

  private static final Set<ENGINE> GENERIC_ENGINE_PARAMETER_ENGINES =
      Collections.unmodifiableSet(
          EnumSet.of(
              ENGINE.REPLACINGMERGETREE,
              ENGINE.SUMMINGMERGETREE,
              ENGINE.COLLAPSINGMERGETREE,
              ENGINE.VERSIONEDCOLLAPSINGMERGETREE));
  private static final Pattern PARTITION_BY_PATTERN =
      Pattern.compile(
          "(?is)\\bPARTITION\\s+BY\\s*(.+?)(?=\\bORDER\\s+BY\\b|\\bPRIMARY\\s+KEY\\b|\\bSAMPLE\\s+BY\\b|\\bTTL\\b|\\bSETTINGS\\b|\\bCOMMENT\\b|$)");
  private static final Pattern SETTINGS_PATTERN =
      Pattern.compile("(?is)\\bSETTINGS\\s+(.+?)(?=\\bCOMMENT\\b|$)");
  private static final Pattern DISTRIBUTED_ENGINE_PATTERN =
      Pattern.compile(
          "(?i)^Distributed\\(([^,]+),\\s*([^,]+),\\s*([^,]+),\\s*(.+)\\)$", Pattern.DOTALL);
  /** Matches ClickHouse wide integer type names (Int128/256, UInt128/256, and future variants). */
  private static final Pattern WIDE_INTEGER_PATTERN = Pattern.compile("^U?INT\\d+$");

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

  private static final String SECONDARY_INDEX_QUERY =
      "SELECT name, type, type_full, expr, granularity FROM system.data_skipping_indices "
          + "WHERE database = ? AND table = ? ORDER BY name";
  private static final String LEGACY_SECONDARY_INDEX_QUERY =
      "SELECT name, type, expr, granularity FROM system.data_skipping_indices "
          + "WHERE database = ? AND table = ? ORDER BY name";

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
    // When columns are provided, delegate directly to the parent implementation.
    if (ArrayUtils.isNotEmpty(columns)) {
      super.create(
          databaseName,
          tableName,
          columns,
          comment,
          properties,
          partitioning,
          distribution,
          indexes,
          sortOrders);
      return;
    }

    // When columns is empty (distributed table using AS remote_table), the shard key validation
    // in handleDistributeTable is skipped. Fetch remote table columns and validate here.
    Map<String, String> props =
        MapUtils.isNotEmpty(properties) ? properties : Collections.emptyMap();
    String engine = props.get(GRAVITINO_ENGINE_KEY);
    if (StringUtils.isNotEmpty(engine) && ENGINE.DISTRIBUTED == ENGINE.fromString(engine)) {
      String shardingKey = props.get(DistributedTableConstants.SHARDING_KEY);
      String remoteDb = props.get(DistributedTableConstants.REMOTE_DATABASE);
      String remoteTbl = props.get(DistributedTableConstants.REMOTE_TABLE);
      if (StringUtils.isNotBlank(shardingKey)) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(remoteDb), "Remote database must be specified for Distributed");
        Preconditions.checkArgument(
            StringUtils.isNotBlank(remoteTbl), "Remote table must be specified for Distributed");
        try (Connection conn = getConnection(databaseName)) {
          JdbcColumn[] remoteCols = fetchRemoteColumns(conn, remoteDb, remoteTbl);
          validateShardKeyColumns(
              remoteCols, shardingKey, "in remote table %s.%s".formatted(remoteDb, remoteTbl));
        } catch (SQLException e) {
          throw exceptionMapper.toGravitinoException(e);
        }
      }
    }
    super.create(
        databaseName,
        tableName,
        columns,
        comment,
        properties,
        partitioning,
        distribution,
        indexes,
        sortOrders);
  }

  private JdbcColumn[] fetchRemoteColumns(Connection conn, String db, String tbl)
      throws SQLException {
    List<JdbcColumn> cols = new ArrayList<>();
    try (ResultSet rs = getColumns(conn, db, tbl)) {
      while (rs.next()) {
        JdbcColumn.Builder b = getColumnBuilder(rs, db, tbl);
        if (b != null) {
          b.withAutoIncrement(getAutoIncrementInfo(rs));
          cols.add(b.build());
        }
      }
    }
    return cols.toArray(new JdbcColumn[0]);
  }

  /**
   * Validates that bare-column shard keys exist, are not nullable, and are integer-typed. Shared by
   * {@link #create} (empty columns) and {@link #handleDistributeTable} (explicit columns).
   */
  private void validateShardKeyColumns(
      JdbcColumn[] columns, String shardingKey, String contextMsg) {
    List<String> shardingColumns = ClickHouseTableSqlUtils.extractShardingKeyColumns(shardingKey);
    if (CollectionUtils.isEmpty(shardingColumns)) {
      return;
    }
    boolean isBareColumn = ClickHouseTableSqlUtils.isSimpleIdentifier(shardingKey.trim());
    for (String columnName : shardingColumns) {
      JdbcColumn col = findColumn(columns, columnName);
      Preconditions.checkArgument(
          col != null, "Sharding key column %s not found %s", columnName, contextMsg);
      if (isBareColumn) {
        Preconditions.checkArgument(
            !col.nullable(), "Sharding key column %s must not be nullable", columnName);
        Preconditions.checkArgument(
            isIntegerType(col.dataType()),
            "Sharding key column %s must be an integer type, but got %s",
            columnName,
            col.dataType());
      }
    }
  }

  @Override
  protected List<Index> getIndexes(Connection connection, String databaseName, String tableName) {
    // cause clickhouse not impl getPrimaryKeys yet, ref:
    // https://github.com/ClickHouse/clickhouse-java/issues/1625
    String sql =
        QUERY_INDEXES_SQL.formatted(
            escapeSingleQuotes(databaseName), escapeSingleQuotes(tableName));
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      List<Index> indexes = new ArrayList<>();
      while (resultSet.next()) {
        String indexName = resultSet.getString("PK_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        indexes.add(
            Indexes.of(Index.IndexType.PRIMARY_KEY, indexName, new String[][] {{columnName}}));
      }
      indexes.addAll(getSecondaryIndexes(connection, databaseName, tableName));
      return indexes;
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  @Override
  public boolean supportsTableSortOrder() {
    return true;
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

    Preconditions.checkArgument(
        Distributions.NONE.equals(distribution), "ClickHouse does not support distribution");

    StringBuilder sqlBuilder = new StringBuilder();

    Map<String, String> notNullProperties =
        MapUtils.isNotEmpty(properties) ? properties : Collections.emptyMap();

    validateNoAutoIncrementColumns(columns);

    // Add Create table clause; capture whether ON CLUSTER is in use
    boolean onCluster = appendCreateTableClause(notNullProperties, sqlBuilder, tableName);

    // We still allow empty columns when the engine is distributed.
    if (columns.length > 0) {
      buildColumnsDefinition(columns, sqlBuilder);

      // Index definition
      appendIndexesSql(indexes, sqlBuilder);

      sqlBuilder.append("\n)");
    }

    // Extract engine from properties
    ClickHouseTablePropertiesMetadata.ENGINE engine =
        appendTableEngine(notNullProperties, sqlBuilder, columns);

    appendOrderBy(sortOrders, sqlBuilder, engine);

    appendPartitionClause(partitioning, sqlBuilder, engine);

    // Add setting clause before COMMENT; ClickHouse 24.8 rejects SETTINGS that follow COMMENT
    // (all settings become UNKNOWN_SETTING when preceded by a COMMENT clause).
    // This matches the order in SHOW CREATE TABLE output: SETTINGS ... COMMENT '...'.
    appendTableProperties(notNullProperties, sqlBuilder);

    // Add table comment; embed cluster name so it can be recovered at DROP/ALTER time.
    // ClickHouse does not persist ON CLUSTER in SHOW CREATE TABLE (see ClickHouseClusterUtils).
    String storedComment =
        onCluster
            ? ClickHouseClusterUtils.embedClusterInComment(
                comment, notNullProperties.get(ClusterConstants.CLUSTER_NAME))
            : comment;
    if (StringUtils.isNotEmpty(storedComment)) {
      sqlBuilder.append(" COMMENT '%s'".formatted(escapeSingleQuotes(storedComment)));
    }

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  /**
   * Append CREATE TABLE clause. If cluster name && on-cluster is specified in properties, append ON
   * CLUSTER clause.
   *
   * @param properties Table properties
   * @param sqlBuilder SQL builder
   * @return true if ON CLUSTER clause is appended, false otherwise
   */
  private boolean appendCreateTableClause(
      Map<String, String> properties, StringBuilder sqlBuilder, String tableName) {
    String clusterName = properties.get(ClusterConstants.CLUSTER_NAME);
    String onClusterValue = properties.get(ClusterConstants.ON_CLUSTER);

    boolean onCluster =
        StringUtils.isNotBlank(clusterName)
            && StringUtils.isNotBlank(onClusterValue)
            && Boolean.TRUE.equals(Boolean.parseBoolean(onClusterValue));

    if (onCluster) {
      sqlBuilder.append(
          "CREATE TABLE %s ON CLUSTER %s \n"
              .formatted(quoteIdentifier(tableName), quoteIdentifier(clusterName)));
    } else {
      sqlBuilder.append("CREATE TABLE %s \n".formatted(quoteIdentifier(tableName)));
    }

    return onCluster;
  }

  private static void appendTableProperties(
      Map<String, String> properties, StringBuilder sqlBuilder) {
    if (MapUtils.isEmpty(properties)) {
      return;
    }

    Map<String, String> settingMap =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(TableConstants.SETTINGS_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (MapUtils.isEmpty(settingMap)) {
      return;
    }

    String settings =
        settingMap.entrySet().stream()
            .map(
                entry ->
                    entry.getKey().substring(TableConstants.SETTINGS_PREFIX.length())
                        + " = "
                        + entry.getValue())
            .collect(Collectors.joining(",\n ", " \n SETTINGS ", ""));
    sqlBuilder.append(settings);
  }

  private void appendOrderBy(
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

    List<String> orderBySql = new ArrayList<>();
    for (SortOrder sortOrder : sortOrders) {
      NullOrdering nullOrdering = sortOrder.nullOrdering();
      SortDirection sortDirection = sortOrder.direction();
      if (nullOrdering != null) {
        LOG.warn(
            "ClickHouse currently does not support nullOrdering: {}, and will ignore it",
            nullOrdering);
      }

      String exprSql = toOrderBySql(sortOrder.expression());
      if (sortDirection == SortDirection.DESCENDING) {
        exprSql = exprSql + " DESC";
      }
      orderBySql.add(exprSql);
    }

    String renderedOrderBy =
        orderBySql.size() == 1 ? orderBySql.get(0) : "(" + String.join(", ", orderBySql) + ")";
    sqlBuilder.append("\n ORDER BY ").append(renderedOrderBy).append("\n");
  }

  private ClickHouseTablePropertiesMetadata.ENGINE appendTableEngine(
      Map<String, String> properties, StringBuilder sqlBuilder, JdbcColumn[] columns) {
    ClickHouseTablePropertiesMetadata.ENGINE engine = ENGINE_PROPERTY_ENTRY.getDefaultValue();
    if (MapUtils.isNotEmpty(properties)) {
      String userSetEngine = properties.get(GRAVITINO_ENGINE_KEY);
      if (StringUtils.isNotEmpty(userSetEngine)) {
        engine = ClickHouseTablePropertiesMetadata.ENGINE.fromString(userSetEngine);
      }
    }

    String engineParams = StringUtils.trim(properties.get(TableConstants.ENGINE_PARAMETERS));
    validateEngineParameters(engine, engineParams);

    if (engine == ENGINE.DISTRIBUTED) {
      handleDistributeTable(properties, sqlBuilder, columns);
      return engine;
    }

    if (engine == ENGINE.GRAPHITEMERGETREE) {
      String config = properties.get(TableConstants.GRAPHITE_CONFIG);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(config),
          "GraphiteMergeTree requires '%s' property referencing a <graphite_rollup> config element",
          TableConstants.GRAPHITE_CONFIG);
      String escapedConfig = JdbcConnectorUtils.escapeSqlLiteral(config, '\'');
      sqlBuilder.append("\n ENGINE = GraphiteMergeTree('%s')".formatted(escapedConfig));
      return engine;
    }

    if (StringUtils.isNotBlank(engineParams)) {
      sqlBuilder.append("\n ENGINE = %s(%s)".formatted(engine.getValue(), engineParams));
    } else {
      sqlBuilder.append("\n ENGINE = %s".formatted(engine.getValue()));
    }
    return engine;
  }

  private void handleDistributeTable(
      Map<String, String> properties, StringBuilder sqlBuilder, JdbcColumn[] columns) {

    // Check properties
    String clusterName = properties.get(ClusterConstants.CLUSTER_NAME);
    String remoteDatabase = properties.get(DistributedTableConstants.REMOTE_DATABASE);
    String remoteTable = properties.get(DistributedTableConstants.REMOTE_TABLE);
    String shardingKey = properties.get(DistributedTableConstants.SHARDING_KEY);

    Preconditions.checkArgument(
        StringUtils.isNotBlank(clusterName),
        "Cluster name must be specified when engine is Distributed");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(remoteDatabase),
        "Remote database must be specified for Distributed");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(remoteTable), "Remote table must be specified for Distributed");

    // User must ensure the sharding key is a trusted value.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(shardingKey), "Sharding key must be specified for Distributed");

    // Users have defined the columns explicitly for the distributed table, we will check the
    // columns should contain the sharding key, as clickhouse requires the sharding key must be
    // defined in the columns of the distributed table.
    if (ArrayUtils.isNotEmpty(columns)) {
      validateShardKeyColumns(columns, shardingKey, "in the table");
    }

    if (ArrayUtils.isEmpty(columns)) {
      sqlBuilder.append(" AS `%s`.`%s` ".formatted(remoteDatabase, remoteTable));
    }

    String sanitizedShardingKey = ClickHouseTableSqlUtils.formatShardingKey(shardingKey);
    sqlBuilder.append(
        " ENGINE = %s(`%s`,`%s`,`%s`,%s)"
            .formatted(
                ENGINE.DISTRIBUTED.getValue(),
                clusterName,
                remoteDatabase,
                remoteTable,
                sanitizedShardingKey));
  }

  private void appendPartitionClause(
      Transform[] partitioning,
      StringBuilder sqlBuilder,
      ClickHouseTablePropertiesMetadata.ENGINE engine) {
    if (ArrayUtils.isEmpty(partitioning)) {
      return;
    }

    if (!engine.acceptPartition()) {
      throw new UnsupportedOperationException(
          "Partitioning is only supported for MergeTree family engines");
    }

    List<String> partitionExprs =
        Arrays.stream(partitioning)
            .map(ClickHouseTableSqlUtils::toPartitionExpression)
            .collect(Collectors.toList());
    String partitionExpr =
        partitionExprs.size() == 1
            ? partitionExprs.get(0)
            : "tuple(" + String.join(", ", partitionExprs) + ")";
    sqlBuilder.append("\n PARTITION BY ").append(partitionExpr);
  }

  private JdbcColumn findColumn(JdbcColumn[] columns, String columnName) {
    if (ArrayUtils.isEmpty(columns)) {
      return null;
    }

    return Arrays.stream(columns)
        .filter(column -> StringUtils.equals(column.name(), columnName))
        .findFirst()
        .orElse(null);
  }

  private void validateNoAutoIncrementColumns(JdbcColumn[] columns) {
    if (ArrayUtils.isEmpty(columns)) {
      return;
    }

    for (JdbcColumn column : columns) {
      if (column.autoIncrement()) {
        throw new UnsupportedOperationException(
            "ClickHouse does not support auto increment column: '%s' in CREATE TABLE"
                .formatted(column.name()));
      }
    }
  }

  private void buildColumnsDefinition(JdbcColumn[] columns, StringBuilder sqlBuilder) {
    if (ArrayUtils.isEmpty(columns)) {
      return;
    }

    sqlBuilder.append(" (");
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

  /**
   * ClickHouse supports primary key and data skipping indexes.
   *
   * <p>This method will not check the validity of the indexes. For ClickHouse, the primary key must
   * be a subset of the order by columns. We will leave the underlying clickhouse to validate it.
   */
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
          // fieldStr already quoted in getIndexFieldStr
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case DATA_SKIPPING_MINMAX:
        case DATA_SKIPPING_BLOOM_FILTER:
        case DATA_SKIPPING_SET:
        case DATA_SKIPPING_NGRAMBFV1:
        case DATA_SKIPPING_TOKENBFV1:
          sqlBuilder
              .append(" ")
              .append(
                  buildDataSkippingIndexDdl(
                      index.name(), fieldStr, index.type(), index.properties()));
          break;
        default:
          throw new IllegalArgumentException(
              "Gravitino Clickhouse doesn't support index : " + index.type());
      }
    }
  }

  /**
   * Checks whether the given type represents an integer type suitable for shard keys. Covers both
   * Gravitino's built-in integral types (Int8-Int64, UInt8-UInt64) and ClickHouse-specific wide
   * integers (Int128/256, UInt128/256) that map to {@link Types.ExternalType}. The regex matches
   * the ClickHouse naming convention {@code U?INT<width>} to automatically cover future integer
   * variants (e.g. Int512) without code changes.
   */
  private static boolean isIntegerType(Type type) {
    if (type instanceof Type.IntegralType) {
      return true;
    }
    if (type instanceof Types.ExternalType ext) {
      return WIDE_INTEGER_PATTERN.matcher(ext.catalogString().toUpperCase(Locale.ROOT)).matches();
    }
    return false;
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT"));
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
  public void rename(String databaseName, String oldTableName, String newTableName)
      throws NoSuchTableException {
    LOG.info(
        "Attempting to rename table {}/{} to {}/{}",
        databaseName,
        oldTableName,
        databaseName,
        newTableName);
    try (Connection connection = getConnection(databaseName)) {
      TablePropertiesWithClusterMetadata metadata =
          loadTablePropertiesWithClusterMetadata(connection, oldTableName);
      JdbcConnectorUtils.executeUpdate(
          connection,
          generateRenameTableSql(
              oldTableName, newTableName, metadata.hasClusterMetadata(), metadata.clusterName()));
      LOG.info(
          "Renamed table {}/{} to {}/{}", databaseName, oldTableName, databaseName, newTableName);
    } catch (final SQLException se) {
      throw exceptionMapper.toGravitinoException(se);
    }
  }

  @VisibleForTesting
  String generateRenameTableSql(
      String oldTableName,
      String newTableName,
      boolean hasClusterMetadata,
      @Nullable String clusterName) {
    if (hasClusterMetadata) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(clusterName),
          "ClickHouse cluster metadata for table %s is missing a cluster name",
          oldTableName);
      return "RENAME TABLE %s TO %s ON CLUSTER %s"
          .formatted(
              quoteIdentifier(oldTableName),
              quoteIdentifier(newTableName),
              quoteIdentifier(clusterName));
    }
    return "RENAME TABLE %s TO %s"
        .formatted(quoteIdentifier(oldTableName), quoteIdentifier(newTableName));
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    return loadTablePropertiesWithClusterMetadata(connection, tableName).properties();
  }

  private TablePropertiesWithClusterMetadata loadTablePropertiesWithClusterMetadata(
      Connection connection, String tableName) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT comment, engine, engine_full FROM system.tables "
                + "WHERE database = currentDatabase() AND name = ?")) {
      statement.setString(1, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchTableException(
              "Table %s does not exist in %s.", tableName, connection.getCatalog());
        }

        Map<String, String> tableProperties = new HashMap<>();

        // Extract cluster name embedded in the COMMENT at create time.
        // SHOW CREATE TABLE does not include ON CLUSTER (see ClickHouseClusterUtils).
        String storedComment = resultSet.getString(COMMENT);
        boolean hasClusterMetadata = ClickHouseClusterUtils.hasClusterMetadata(storedComment);
        String clusterName = ClickHouseClusterUtils.extractClusterFromComment(storedComment);
        tableProperties.put(COMMENT, ClickHouseClusterUtils.stripClusterMetadata(storedComment));
        String engine = resultSet.getString(CLICKHOUSE_ENGINE_KEY);
        String engineFull = resultSet.getString("engine_full");
        tableProperties.put(GRAVITINO_ENGINE_KEY, engine);
        if (StringUtils.isNotBlank(clusterName)) {
          tableProperties.put(ClusterConstants.ON_CLUSTER, String.valueOf(true));
          tableProperties.put(ClusterConstants.CLUSTER_NAME, clusterName);
        } else {
          tableProperties.put(ClusterConstants.ON_CLUSTER, String.valueOf(false));
        }

        if (StringUtils.equalsIgnoreCase(engine, ENGINE.DISTRIBUTED.getValue())) {
          Matcher distributedEngineMatcher =
              DISTRIBUTED_ENGINE_PATTERN.matcher(StringUtils.trimToEmpty(engineFull));
          if (distributedEngineMatcher.matches()) {
            String distributedClusterName = unquote(distributedEngineMatcher.group(1));
            tableProperties.put(ClusterConstants.CLUSTER_NAME, distributedClusterName);
            tableProperties.put(
                DistributedTableConstants.REMOTE_DATABASE,
                unquote(distributedEngineMatcher.group(2)));
            tableProperties.put(
                DistributedTableConstants.REMOTE_TABLE, unquote(distributedEngineMatcher.group(3)));
            tableProperties.put(
                DistributedTableConstants.SHARDING_KEY,
                StringUtils.trim(distributedEngineMatcher.group(4)));
          }
        } else if (StringUtils.equalsIgnoreCase(engine, ENGINE.GRAPHITEMERGETREE.getValue())) {
          String graphiteConfig = extractGraphiteConfig(engineFull);
          if (StringUtils.isNotBlank(graphiteConfig)) {
            tableProperties.put(TableConstants.GRAPHITE_CONFIG, graphiteConfig);
          }
        } else if (isGenericEngineParameterEngine(engine)) {
          String engineParams = extractEngineParams(engine, engineFull);
          if (StringUtils.isNotBlank(engineParams)) {
            tableProperties.put(TableConstants.ENGINE_PARAMETERS, engineParams);
          }
        }

        return new TablePropertiesWithClusterMetadata(
            Collections.unmodifiableMap(tableProperties), hasClusterMetadata, clusterName);
      }
    }
  }

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    try (Connection connection = getConnection(databaseName)) {
      ResultSet tables = getTable(connection, databaseName, tableName);
      JdbcTable.Builder jdbcTableBuilder = getTableBuilder(tables, databaseName, tableName);

      // Query system.columns for default_kind to correctly identify MATERIALIZED/ALIAS columns.
      // The ClickHouse JDBC driver hardcodes IS_GENERATEDCOLUMN to 'NO' for all columns.
      // Stored as a local variable (not instance field) to avoid thread-safety issues,
      // since ClickHouseTableOperations is a shared singleton across concurrent requests.
      Map<String, String> defaultKinds = getDefaultKinds(connection, databaseName, tableName);

      // NOTE: Cannot use getColumnBuilder() here because we need to override the default
      // value for MATERIALIZED/ALIAS columns between getBasicJdbcColumnInfo() and build().
      List<JdbcColumn> jdbcColumns = new ArrayList<>();
      ResultSet columns = getColumns(connection, databaseName, tableName);
      while (columns.next()) {
        if (!Objects.equals(columns.getString("TABLE_NAME"), tableName)) {
          continue;
        }
        JdbcColumn.Builder columnBuilder = getBasicJdbcColumnInfo(columns);
        // Correct default value for MATERIALIZED/ALIAS columns: the JDBC driver
        // hardcodes IS_GENERATEDCOLUMN to 'NO', so re-derive with isExpression=true.
        String columnName = columns.getString("COLUMN_NAME");
        String defaultKind = defaultKinds.getOrDefault(columnName, "");
        if ("MATERIALIZED".equals(defaultKind) || "ALIAS".equals(defaultKind)) {
          String columnDef = columns.getString("COLUMN_DEF");
          boolean nullable = columns.getBoolean("NULLABLE");
          String typeName = columns.getString("TYPE_NAME");
          int columnSize = columns.getInt("COLUMN_SIZE");
          int scale = columns.getInt("DECIMAL_DIGITS");
          JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean(typeName);
          typeBean.setColumnSize(columnSize);
          typeBean.setScale(scale);
          typeBean.setDatetimePrecision(calculateDatetimePrecision(typeName, columnSize, scale));
          Expression correctDefault =
              columnDefaultValueConverter.toGravitino(typeBean, columnDef, true, nullable);
          columnBuilder.withDefaultValue(correctDefault);
        }
        boolean autoIncrement = getAutoIncrementInfo(columns);
        columnBuilder.withAutoIncrement(autoIncrement);
        jdbcColumns.add(columnBuilder.build());
      }
      jdbcTableBuilder.withColumns(jdbcColumns.toArray(new JdbcColumn[0]));

      List<Index> indexes = getIndexes(connection, databaseName, tableName);
      jdbcTableBuilder.withIndexes(indexes.toArray(new Index[0]));

      SystemTableMetadata systemTableMetadata =
          getSystemTableMetadata(connection, databaseName, tableName);
      ShowCreateTableMetadata showCreateMetadata = parseShowCreateTable(connection, tableName);
      Transform[] partitioning = showCreateMetadata.partitioning;
      if (ArrayUtils.isEmpty(partitioning)) {
        partitioning = getTablePartitioning(connection, databaseName, tableName);
      }
      jdbcTableBuilder.withPartitioning(partitioning);
      jdbcTableBuilder.withSortOrders(systemTableMetadata.sortOrders());

      Distribution distribution = getDistributionInfo(connection, databaseName, tableName);
      jdbcTableBuilder.withDistribution(distribution);

      Map<String, String> tableProperties = getTableProperties(connection, tableName);
      // Merge SETTINGS parsed from system.tables.engine_full into table properties.
      // engine_full contains only table-level storage clauses, so projection SETTINGS cannot be
      // mistaken for table SETTINGS. These values take precedence
      // over any settings.* keys that might exist in system.tables (though getTableProperties()
      // currently does not read SETTINGS from system.tables, so no overlap occurs in practice).
      if (!systemTableMetadata.settings().isEmpty()) {
        Map<String, String> merged = new HashMap<>(tableProperties);
        merged.putAll(systemTableMetadata.settings());
        tableProperties = Collections.unmodifiableMap(merged);
      }
      jdbcTableBuilder.withProperties(tableProperties);

      correctJdbcTableFields(connection, databaseName, tableName, jdbcTableBuilder);

      return jdbcTableBuilder.withTableOperation(this).build();
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  @VisibleForTesting
  Map<String, String> getDefaultKinds(Connection connection, String database, String table)
      throws SQLException {
    Map<String, String> kinds = new HashMap<>();
    String sql = "SELECT name, default_kind FROM system.columns WHERE database = ? AND table = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, database);
      stmt.setString(2, table);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          kinds.put(rs.getString("name"), rs.getString("default_kind"));
        }
      }
    }
    return kinds;
  }

  @VisibleForTesting
  SystemTableMetadata getSystemTableMetadata(
      Connection connection, String databaseName, String tableName) throws SQLException {
    String sql =
        "SELECT sorting_key, engine_full FROM system.tables WHERE database = ? AND name = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, databaseName);
      statement.setString(2, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          return new SystemTableMetadata(
              parseOrderByClause(resultSet.getString("sorting_key")),
              parseSettingsFromEngineFull(resultSet.getString("engine_full")));
        }
      }
    }

    throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
  }

  @Override
  protected Transform[] getTablePartitioning(
      Connection connection, String databaseName, String tableName) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT partition_key FROM system.tables WHERE database = ? AND name = ?")) {
      statement.setString(1, databaseName);
      statement.setString(2, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          String partitionKey = resultSet.getString("partition_key");
          try {
            return parsePartitioning(partitionKey);
          } catch (IllegalArgumentException | UnsupportedOperationException e) {
            LOG.warn(
                "Skip unsupported partition expression {} for {}.{}",
                partitionKey,
                databaseName,
                tableName);
            return Transforms.EMPTY_TRANSFORM;
          }
        }
      }
    }

    return Transforms.EMPTY_TRANSFORM;
  }

  @Override
  protected ResultSet getColumns(Connection connection, String databaseName, String tableName)
      throws SQLException {
    // The parent implementation ignores databaseName and uses connection.getSchema(), which is
    // incorrect for ClickHouse when the target database differs from the connection's default
    // database (e.g., Distributed tables referencing a remote database). Pass databaseName as
    // the schema pattern so JDBC metadata is filtered by the intended database.
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(connection.getCatalog(), databaseName, tableName, null);
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
  protected void dropTable(String databaseName, String tableName) {
    LOG.info("Attempting to delete table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      Map<String, String> props = getTableProperties(connection, tableName);
      JdbcConnectorUtils.executeUpdate(connection, generateDropTableSql(tableName, props));
      LOG.info("Deleted table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * Generates the SQL statement to drop a ClickHouse table. When the table was created with {@code
   * ON CLUSTER}, the DROP statement includes {@code ON CLUSTER `clusterName` SYNC} so the operation
   * is propagated to every cluster node synchronously.
   *
   * @param tableName The name of the table to drop.
   * @param properties The table properties as returned by {@link #getTableProperties}; used to
   *     determine whether the table is on a cluster.
   * @return The DROP TABLE SQL statement.
   */
  @VisibleForTesting
  String generateDropTableSql(String tableName, Map<String, String> properties) {
    String clusterName = properties == null ? null : properties.get(ClusterConstants.CLUSTER_NAME);
    boolean onCluster =
        properties != null
            && Boolean.parseBoolean(properties.getOrDefault(ClusterConstants.ON_CLUSTER, "false"));

    if (onCluster && StringUtils.isNotBlank(clusterName)) {
      return String.format(
          "DROP TABLE %s ON CLUSTER %s SYNC",
          quoteIdentifier(tableName), quoteIdentifier(clusterName));
    }
    return String.format("DROP TABLE %s", quoteIdentifier(tableName));
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "ClickHouse does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    // Not all operations require the original table information, so lazy loading is used here
    JdbcTable lazyLoadTable = null;
    TableChange.UpdateComment updateComment = null;
    List<TableChange.SetProperty> setProperties = new ArrayList<>();
    List<String> alterSql = new ArrayList<>();

    for (TableChange change : changes) {
      if (change instanceof TableChange.UpdateComment) {
        updateComment = (TableChange.UpdateComment) change;

      } else if (change instanceof TableChange.SetProperty setProperty) {
        // The set attribute needs to be added at the end.
        setProperties.add(setProperty);

      } else if (change instanceof TableChange.RemoveProperty) {
        // Clickhouse does not support deleting table attributes, it can be replaced by Set Property
        throw new UnsupportedOperationException(
            "Remove property for ClickHouse is not supported yet");

      } else if (change instanceof TableChange.AddColumn addColumn) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addColumnFieldDefinition(addColumn));

      } else if (change instanceof TableChange.RenameColumn renameColumn) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(renameColumnFieldDefinition(renameColumn));

      } else if (change instanceof TableChange.UpdateColumnDefaultValue updateColumnDefaultValue) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(
            updateColumnDefaultValueFieldDefinition(updateColumnDefaultValue, lazyLoadTable));

      } else if (change instanceof TableChange.UpdateColumnType updateColumnType) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));

      } else if (change instanceof TableChange.UpdateColumnComment updateColumnComment) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnCommentFieldDefinition(updateColumnComment, lazyLoadTable));

      } else if (change instanceof TableChange.UpdateColumnPosition updateColumnPosition) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(updateColumnPositionFieldDefinition(updateColumnPosition, lazyLoadTable));

      } else if (change instanceof TableChange.DeleteColumn deleteColumn) {
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

      } else if (change instanceof TableChange.AddIndex addIndex) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(addIndexDefinition(lazyLoadTable, addIndex));

      } else if (change instanceof TableChange.DeleteIndex) {
        lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
        alterSql.add(deleteIndexDefinition(lazyLoadTable, (TableChange.DeleteIndex) change));

      } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
        // Auto increment functionality was added in ClickHouse 25.1. Since this PR is based on
        // 23.x, we throw unsupported operation here.
        throw new UnsupportedOperationException(
            "ClickHouse auto increment is not supported in this version.");
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type: " + change.getClass().getName());
      }
    }

    // Last modified comment
    if (null != updateComment) {
      String newComment = updateComment.getNewComment();
      // Load the existing table once. We need it for two purposes:
      //   1. Preserve the Gravitino StringIdentifier embedded in the old comment, so Gravitino can
      //      still identify the table after the comment is changed.
      //   2. Re-embed the cluster name so it is not lost. ClickHouse does not persist ON CLUSTER
      //      in SHOW CREATE TABLE, so the cluster name lives only in the stored comment.
      lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
      if (null == StringIdentifier.fromComment(newComment)) {
        StringIdentifier identifier = StringIdentifier.fromComment(lazyLoadTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
      String clusterName = lazyLoadTable.properties().get(ClusterConstants.CLUSTER_NAME);
      if (StringUtils.isNotBlank(clusterName)) {
        newComment = ClickHouseClusterUtils.embedClusterInComment(newComment, clusterName);
      }
      alterSql.add(" MODIFY COMMENT '%s'".formatted(escapeSingleQuotes(newComment)));
    }

    if (!setProperties.isEmpty()) {
      alterSql.add(generateAlterTableProperties(setProperties));
    }

    // Remove all empty SQL statements
    List<String> nonEmptySQLs =
        alterSql.stream().filter(StringUtils::isNotEmpty).collect(Collectors.toList());
    if (CollectionUtils.isEmpty(nonEmptySQLs)) {
      return "";
    }

    // Check if the table is on a cluster, so that ALTER TABLE includes ON CLUSTER
    lazyLoadTable = getOrCreateTable(databaseName, tableName, lazyLoadTable);
    Map<String, String> props = lazyLoadTable.properties();
    String clusterName = props == null ? null : props.get(ClusterConstants.CLUSTER_NAME);
    boolean onCluster =
        props != null
            && Boolean.parseBoolean(props.getOrDefault(ClusterConstants.ON_CLUSTER, "false"));

    // Return the generated SQL statement
    String result;
    if (onCluster && StringUtils.isNotBlank(clusterName)) {
      result =
          "ALTER TABLE %s ON CLUSTER %s \n%s;"
              .formatted(
                  quoteIdentifier(tableName),
                  quoteIdentifier(clusterName),
                  String.join(",\n", nonEmptySQLs));
    } else {
      result =
          "ALTER TABLE %s \n%s;"
              .formatted(quoteIdentifier(tableName), String.join(",\n", nonEmptySQLs));
    }
    LOG.info("Generated alter table:{} sql: {}", databaseName + "." + tableName, result);
    return result;
  }

  private String addIndexDefinition(JdbcTable table, TableChange.AddIndex addIndex) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(addIndex.getName()), "Index name is required");
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(addIndex.getFieldNames()), "Index field names are required");

    boolean indexExists =
        Arrays.stream(table.index()).anyMatch(index -> index.name().equals(addIndex.getName()));
    Preconditions.checkArgument(!indexExists, "Index '%s' already exists", addIndex.getName());

    String fieldStr = getIndexFieldStr(addIndex.getFieldNames());
    Map<String, String> properties = addIndex.getProperties();
    switch (addIndex.getType()) {
      case DATA_SKIPPING_MINMAX:
      case DATA_SKIPPING_BLOOM_FILTER:
      case DATA_SKIPPING_SET:
      case DATA_SKIPPING_NGRAMBFV1:
      case DATA_SKIPPING_TOKENBFV1:
        return "ADD "
            + buildDataSkippingIndexDdl(
                addIndex.getName(), fieldStr, addIndex.getType(), properties);

      case PRIMARY_KEY:
        throw new UnsupportedOperationException(
            "ClickHouse does not support adding primary key via ALTER TABLE");

      default:
        throw new IllegalArgumentException(
            "Gravitino ClickHouse doesn't support index : " + addIndex.getType());
    }
  }

  /**
   * Resolves an integer property from the index properties map.
   *
   * @param properties the index properties map
   * @param key the property key (e.g. {@link ClickHouseConstants.IndexConstants#GRANULARITY})
   * @param defaultValue the value returned when the key is absent
   * @param minValue the minimum allowed value (inclusive)
   * @return the resolved integer value
   * @throws IllegalArgumentException if the value is present but not a valid integer within bounds
   */
  private int resolveIntProperty(
      Map<String, String> properties, String key, int defaultValue, int minValue) {
    if (properties == null) {
      return defaultValue;
    }
    String raw = properties.get(key);
    if (raw == null) {
      return defaultValue;
    }
    raw = raw.trim();
    int value;
    try {
      value = Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be a valid integer, but got: " + raw, e);
    }
    Preconditions.checkArgument(
        value >= minValue, "%s must be >= %s, but got: %s", key, minValue, value);
    return value;
  }

  private int resolveGranularity(Map<String, String> properties, int defaultValue) {
    return resolveIntProperty(properties, GRANULARITY, defaultValue, 1);
  }

  private int resolveSetMaxValues(Map<String, String> properties) {
    return resolveIntProperty(properties, SET_MAX_VALUES, 0, 0);
  }

  @VisibleForTesting
  private String deleteIndexDefinition(
      JdbcTable lazyLoadTable, TableChange.DeleteIndex deleteIndex) {
    boolean indexExists =
        Arrays.stream(lazyLoadTable.index())
            .anyMatch(index -> index.name().equals(deleteIndex.getName()));

    // Index does not exist
    if (!indexExists) {
      // If ifExists is true, return empty string to skip the operation
      if (deleteIndex.isIfExists()) {
        return "";
      } else {
        throw new IllegalArgumentException(
            "Index '%s' does not exist".formatted(deleteIndex.getName()));
      }
    }

    return "DROP INDEX %s ".formatted(quoteIdentifier(deleteIndex.getName()));
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

    return "%s %s %s"
        .formatted(
            MODIFY_COLUMN,
            quoteIdentifier(col),
            appendColumnDefinition(updateColumn, new StringBuilder()));
  }

  private String generateAlterTableProperties(List<TableChange.SetProperty> setProperties) {
    if (CollectionUtils.isNotEmpty(setProperties)) {
      throw new UnsupportedOperationException(
          "Alter table properties in ClickHouse is not supported");
    }

    return "";
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, JdbcTable jdbcTable) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = updateColumnComment.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    JdbcColumn updateColumn =
        JdbcColumn.builder()
            .withName(col)
            .withDefaultValue(column.defaultValue())
            .withNullable(column.nullable())
            .withType(column.dataType())
            .withComment(newComment)
            .withAutoIncrement(column.autoIncrement())
            .build();

    return "%s %s %s"
        .formatted(
            MODIFY_COLUMN,
            quoteIdentifier(col),
            appendColumnDefinition(updateColumn, new StringBuilder()));
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    String dataType = typeConverter.fromGravitino(addColumn.getDataType());
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = addColumn.fieldName()[0];
    StringBuilder columnDefinition = new StringBuilder();
    //  [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
    if (addColumn.isNullable()) {
      columnDefinition.append(
          "ADD COLUMN %s Nullable(%s) ".formatted(quoteIdentifier(col), dataType));
    } else {
      columnDefinition.append("ADD COLUMN %s %s ".formatted(quoteIdentifier(col), dataType));
    }

    if (addColumn.isAutoIncrement()) {
      throw new UnsupportedOperationException(
          "ClickHouse does not support adding auto increment column");
    }

    // Append default value if available
    if (!Column.DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      columnDefinition.append(
          "DEFAULT %s "
              .formatted(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue())));
    }

    // Append comment if available after default value
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      String escapedComment = StringUtils.replace(addColumn.getComment(), "'", "''");
      columnDefinition.append(" COMMENT '%s' ".formatted(escapedComment));
    }

    // Append position if available
    if (addColumn.getPosition() instanceof TableChange.First) {
      columnDefinition.append(" FIRST ");
    } else if (addColumn.getPosition() instanceof TableChange.After afterPosition) {
      columnDefinition.append(" AFTER %s ".formatted(quoteIdentifier(afterPosition.getColumn())));
    }

    return columnDefinition.toString();
  }

  private String renameColumnFieldDefinition(TableChange.RenameColumn renameColumn) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String oldColumnName = renameColumn.fieldName()[0];
    String newColumnName = renameColumn.getNewName();

    return "RENAME COLUMN %s TO %s"
        .formatted(quoteIdentifier(oldColumnName), quoteIdentifier(newColumnName));
  }

  private String updateColumnPositionFieldDefinition(
      TableChange.UpdateColumnPosition updateColumnPosition, JdbcTable jdbcTable) {
    if (updateColumnPosition.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = updateColumnPosition.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition.append(" %s %s ".formatted(MODIFY_COLUMN, quoteIdentifier(col)));
    appendColumnDefinition(column, columnDefinition);

    if (updateColumnPosition.getPosition() instanceof TableChange.First) {
      columnDefinition.append(" FIRST ");
    } else if (updateColumnPosition.getPosition() instanceof TableChange.After afterPosition) {
      columnDefinition.append(
          " %s %s ".formatted(AFTER, quoteIdentifier(afterPosition.getColumn())));
    } else {
      Arrays.stream(jdbcTable.columns())
          .reduce((column1, column2) -> column2)
          .map(Column::name)
          .ifPresent(s -> columnDefinition.append(" %s %s ".formatted(AFTER, quoteIdentifier(s))));
    }
    return columnDefinition.toString();
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable jdbcTable) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = deleteColumn.fieldName()[0];
    boolean colExists = columnExists(jdbcTable, col);
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column '%s' does not exist.".formatted(col));
      }
    }

    return "DROP COLUMN %s".formatted(quoteIdentifier(col));
  }

  private String updateColumnDefaultValueFieldDefinition(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, JdbcTable jdbcTable) {
    if (updateColumnDefaultValue.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = updateColumnDefaultValue.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder = new StringBuilder(MODIFY_COLUMN + quoteIdentifier(col));
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName(col)
            .withType(column.dataType())
            .withNullable(column.nullable())
            .withComment(column.comment())
            .withDefaultValue(updateColumnDefaultValue.getNewDefaultValue())
            .build();

    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException(CLICKHOUSE_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }

    String col = updateColumnType.fieldName()[0];
    JdbcColumn column = getJdbcColumnFromTable(jdbcTable, col);
    StringBuilder sqlBuilder =
        new StringBuilder("%s %s ".formatted(MODIFY_COLUMN, quoteIdentifier(col)));

    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName(col)
            .withType(updateColumnType.getNewDataType())
            .withComment(column.comment())
            .withDefaultValue(column.defaultValue())
            .withNullable(column.nullable())
            .withAutoIncrement(column.autoIncrement())
            .build();
    return appendColumnDefinition(newColumn, sqlBuilder).toString();
  }

  @VisibleForTesting
  Transform[] parsePartitioning(String partitionKey) {
    return ClickHouseTableSqlUtils.parsePartitioning(partitionKey);
  }

  private ShowCreateTableMetadata parseCreateStatement(String createSql) {
    ShowCreateTableMetadata metadata = new ShowCreateTableMetadata();
    if (StringUtils.isBlank(createSql)) {
      return metadata;
    }

    Matcher partitionMatcher = PARTITION_BY_PATTERN.matcher(createSql);
    if (partitionMatcher.find()) {
      metadata.partitioning = parsePartitioning(partitionMatcher.group(1));
    }

    return metadata;
  }

  // Parses "key1 = val1, key2 = val2" from a SETTINGS clause.
  // Keys are prefixed with "settings." to match the write path convention in
  // appendTableProperties(). ClickHouse SETTINGS values are scalar (UInt64, Bool,
  // String, Enum) — arrays or nested structures are not valid SETTINGS values,
  // so splitting by comma is safe.
  private static Map<String, String> parseSettingsClause(String settingsStr) {
    Map<String, String> settings = new HashMap<>();
    for (String pair : settingsStr.split(",")) {
      String trimmed = pair.trim();
      int eqIdx = trimmed.indexOf('=');
      if (eqIdx > 0) {
        String key = trimmed.substring(0, eqIdx).trim();
        String value = trimmed.substring(eqIdx + 1).trim();
        settings.put(TableConstants.SETTINGS_PREFIX + key, value);
      }
    }
    return settings;
  }

  @VisibleForTesting
  Map<String, String> parseSettingsFromEngineFull(String engineFull) {
    if (StringUtils.isBlank(engineFull)) {
      return Collections.emptyMap();
    }

    Matcher settingsMatcher = SETTINGS_PATTERN.matcher(engineFull);
    if (settingsMatcher.find()) {
      return parseSettingsClause(settingsMatcher.group(1));
    }
    return Collections.emptyMap();
  }

  private ShowCreateTableMetadata parseShowCreateTable(Connection connection, String tableName)
      throws SQLException {
    String createSql = parseShowCreateTableSql(connection, tableName);
    return parseCreateStatement(createSql);
  }

  private String parseShowCreateTableSql(Connection connection, String tableName)
      throws SQLException {
    String sql = "SHOW CREATE TABLE " + quoteIdentifier(tableName);
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      if (resultSet.next()) {
        return resultSet.getString(1);
      }
      throw new SQLException("SHOW CREATE TABLE returned no rows for " + tableName);
    }
  }

  private String unquote(String value) {
    String trimmed = StringUtils.trimToEmpty(value);
    if (StringUtils.length(trimmed) >= 2) {
      char first = trimmed.charAt(0);
      char last = trimmed.charAt(trimmed.length() - 1);
      if ((first == '\'' && last == '\'') || (first == '`' && last == '`')) {
        return trimmed.substring(1, trimmed.length() - 1);
      }
    }
    return trimmed;
  }

  private SortOrder[] parseOrderByClause(String orderClause) {
    if (StringUtils.isBlank(orderClause)) {
      return SortOrders.NONE;
    }
    String trimmed = orderClause.trim();
    if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
      trimmed = trimmed.substring(1, trimmed.length() - 1);
    }

    List<String> expressions = splitExpressions(trimmed);
    List<SortOrder> sortOrders = new ArrayList<>();
    for (String expression : expressions) {
      Expression sortExpr = toSortExpression(expression);
      sortOrders.add(SortOrders.of(sortExpr, SortDirection.ASCENDING));
    }
    return sortOrders.toArray(new SortOrder[0]);
  }

  private List<String> splitExpressions(String expressionList) {
    List<String> expressions = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int depth = 0;
    for (int i = 0; i < expressionList.length(); i++) {
      char ch = expressionList.charAt(i);
      if (ch == '(') {
        depth++;
      } else if (ch == ')') {
        depth--;
      } else if (ch == ',' && depth == 0) {
        expressions.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(ch);
    }
    if (current.length() > 0) {
      expressions.add(current.toString());
    }
    return expressions;
  }

  private Expression toSortExpression(String expression) {
    String trimmed = StringUtils.trim(expression);
    if (StringUtils.isBlank(trimmed)) {
      return UnparsedExpression.of(expression);
    }
    String unquoted = trimmed;
    if (StringUtils.startsWith(unquoted, "`") && StringUtils.endsWith(unquoted, "`")) {
      unquoted = unquoted.substring(1, unquoted.length() - 1);
    }
    if (unquoted.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return NamedReference.field(unquoted);
    }

    int firstParen = unquoted.indexOf('(');
    int lastParen = unquoted.lastIndexOf(')');
    if (firstParen > 0 && lastParen > firstParen) {
      String funcName = unquoted.substring(0, firstParen).trim();
      if (funcName.matches("[A-Za-z_][A-Za-z0-9_]*")) {
        String argsString = unquoted.substring(firstParen + 1, lastParen);
        List<String> args = splitExpressions(argsString);
        Expression[] parsedArgs =
            args.stream().map(this::toSortExpression).toArray(Expression[]::new);
        return FunctionExpression.of(funcName, parsedArgs);
      }
    }

    return UnparsedExpression.of(trimmed);
  }

  private String toOrderBySql(Expression expression) {
    if (expression instanceof NamedReference) {
      String[] parts = ((NamedReference) expression).fieldName();
      return "`" + String.join("`.`", parts) + "`";
    } else if (expression instanceof FunctionExpression) {
      FunctionExpression func = (FunctionExpression) expression;
      return func.functionName()
          + Arrays.stream(func.arguments())
              .map(this::toOrderBySql)
              .collect(Collectors.joining(", ", "(", ")"));
    } else if (expression instanceof UnparsedExpression) {
      return ((UnparsedExpression) expression).unparsedExpression();
    }
    return expression.toString();
  }

  @VisibleForTesting
  static final class SystemTableMetadata {
    private final SortOrder[] sortOrders;
    private final Map<String, String> settings;

    private SystemTableMetadata(SortOrder[] sortOrders, Map<String, String> settings) {
      this.sortOrders = sortOrders;
      this.settings = settings;
    }

    SortOrder[] sortOrders() {
      return sortOrders;
    }

    Map<String, String> settings() {
      return settings;
    }
  }

  private static final class ShowCreateTableMetadata {
    private Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
  }

  private static final class TablePropertiesWithClusterMetadata {
    private final Map<String, String> properties;
    private final boolean hasClusterMetadata;
    @Nullable private final String clusterName;

    private TablePropertiesWithClusterMetadata(
        Map<String, String> properties, boolean hasClusterMetadata, @Nullable String clusterName) {
      this.properties = properties;
      this.hasClusterMetadata = hasClusterMetadata;
      this.clusterName = clusterName;
    }

    private Map<String, String> properties() {
      return properties;
    }

    private boolean hasClusterMetadata() {
      return hasClusterMetadata;
    }

    @Nullable
    private String clusterName() {
      return clusterName;
    }
  }

  @VisibleForTesting
  String[][] parseIndexFields(String expression) {
    return ClickHouseTableSqlUtils.parseIndexFields(expression);
  }

  private List<Index> getSecondaryIndexes(
      Connection connection, String databaseName, String tableName) throws SQLException {
    try {
      return querySecondaryIndexes(
          connection, databaseName, tableName, SECONDARY_INDEX_QUERY, true);
    } catch (SQLException e) {
      if (!isMissingTypeFullColumn(e)) {
        throw e;
      }
      LOG.warn(
          "ClickHouse server does not expose system.data_skipping_indices.type_full; "
              + "falling back to the legacy secondary-index query for {}.{}",
          databaseName,
          tableName);
      return querySecondaryIndexes(
          connection, databaseName, tableName, LEGACY_SECONDARY_INDEX_QUERY, false);
    }
  }

  private List<Index> querySecondaryIndexes(
      Connection connection,
      String databaseName,
      String tableName,
      String query,
      boolean includesTypeFull)
      throws SQLException {
    List<Index> secondaryIndexes = new ArrayList<>();
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, databaseName);
      preparedStatement.setString(2, tableName);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          String name = resultSet.getString("name");
          String type = resultSet.getString("type");
          String parameterSource = includesTypeFull ? resultSet.getString("type_full") : type;
          String parameterSourceName = includesTypeFull ? "type_full" : "legacy type";
          String expression = resultSet.getString("expr");
          long granularity = resultSet.getLong("granularity");
          Index.IndexType indexType;
          try {
            indexType = getClickHouseIndexType(type);
          } catch (IllegalArgumentException e) {
            LOG.warn(
                "Skip unsupported data skipping index {} for {}.{} with type {} "
                    + "(parameter metadata={}) and expression {}",
                name,
                databaseName,
                tableName,
                type,
                parameterSource,
                expression,
                e);
            continue;
          }

          Map<String, String> parameterProperties = Collections.emptyMap();
          if (indexType == Index.IndexType.DATA_SKIPPING_SET) {
            try {
              parameterProperties =
                  parseIndexPropertiesForQuery(indexType, parameterSource, name, !includesTypeFull);
            } catch (IllegalArgumentException e) {
              throw new IllegalArgumentException(
                  "Failed to load data skipping index '%s' from %s.%s with %s '%s': %s"
                      .formatted(
                          name,
                          databaseName,
                          tableName,
                          parameterSourceName,
                          parameterSource,
                          e.getMessage()),
                  e);
            }
          }

          String[][] fields;
          try {
            fields = parseIndexFields(expression);
          } catch (IllegalArgumentException e) {
            LOG.warn(
                "Skip unsupported data skipping index {} for {}.{} with type {} "
                    + "(parameter metadata={}) and expression {}",
                name,
                databaseName,
                tableName,
                type,
                parameterSource,
                expression,
                e);
            continue;
          }
          if (ArrayUtils.isEmpty(fields)) {
            continue;
          }

          if (isParameterizedBloomFilterIndex(indexType)) {
            try {
              parameterProperties =
                  parseIndexPropertiesForQuery(indexType, parameterSource, name, !includesTypeFull);
            } catch (IllegalArgumentException e) {
              throw new IllegalArgumentException(
                  "Failed to load data skipping index '%s' from %s.%s with %s '%s': %s"
                      .formatted(
                          name,
                          databaseName,
                          tableName,
                          parameterSourceName,
                          parameterSource,
                          e.getMessage()),
                  e);
            }
          }

          // Only include granularity in properties when it differs from the default,
          // so that indexes created without explicit granularity have empty properties
          // and match the original creation state (avoids false index-change diffs).
          Map<String, String> properties = new HashMap<>();
          if (granularity != DEFAULT_INDEX_GRANULARITY) {
            properties.put(GRANULARITY, String.valueOf(granularity));
          }
          if (!includesTypeFull
              && isParameterizedBloomFilterIndex(indexType)
              && parameterProperties.isEmpty()) {
            LOG.warn(
                "Legacy ClickHouse metadata does not expose bloom-filter parameters for "
                    + "{} index '{}' on {}.{}; loaded Index.properties() is incomplete",
                type,
                name,
                databaseName,
                tableName);
          }
          if (!includesTypeFull
              && indexType == Index.IndexType.DATA_SKIPPING_SET
              && parameterProperties.isEmpty()
              && !StringUtils.contains(parameterSource, "(")) {
            LOG.warn(
                "Legacy ClickHouse metadata does not expose SET max-values parameters for "
                    + "SET index '{}' on {}.{}; loaded Index.properties() is incomplete",
                name,
                databaseName,
                tableName);
          }
          properties.putAll(parameterProperties);
          secondaryIndexes.add(Indexes.of(indexType, name, fields, properties));
        }
      }
    }

    return secondaryIndexes;
  }

  private static boolean isMissingTypeFullColumn(SQLException exception) {
    for (SQLException current = exception; current != null; current = current.getNextException()) {
      for (Throwable cause = current; cause != null; cause = cause.getCause()) {
        String message = StringUtils.lowerCase(cause.getMessage());
        if (message != null
            && message.contains("type_full")
            && (message.contains("unknown")
                || message.contains("missing")
                || message.contains("column"))) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Parses the single positional parameter returned by ClickHouse for a SET data skipping index.
   *
   * @param indexType the mapped Gravitino index type
   * @param typeFull the complete ClickHouse index type expression
   * @param indexName the index name for validation messages
   * @return the SET index properties, or an empty map for non-SET index types and {@code set(0)}
   * @throws IllegalArgumentException if a SET index has malformed or out-of-range parameters
   */
  @VisibleForTesting
  static Map<String, String> parseSetProperties(
      Index.IndexType indexType, String typeFull, String indexName) {
    if (indexType != Index.IndexType.DATA_SKIPPING_SET) {
      return Collections.emptyMap();
    }

    String normalizedTypeFull = StringUtils.trimToEmpty(typeFull);
    int paramsStart = normalizedTypeFull.indexOf('(');
    int paramsEnd = normalizedTypeFull.lastIndexOf(')');
    Preconditions.checkArgument(
        paramsStart > 0 && paramsEnd == normalizedTypeFull.length() - 1,
        "Invalid SET metadata '%s' for index '%s'",
        typeFull,
        indexName);
    Preconditions.checkArgument(
        StringUtils.equalsIgnoreCase(
            DATA_SKIPPING_SET, normalizedTypeFull.substring(0, paramsStart).trim()),
        "SET metadata '%s' does not match SET index '%s'",
        typeFull,
        indexName);

    String[] params = normalizedTypeFull.substring(paramsStart + 1, paramsEnd).split(",", -1);
    Preconditions.checkArgument(
        params.length == 1,
        "Invalid SET metadata '%s' for SET index '%s': expected one parameter but got %s",
        typeFull,
        indexName,
        params.length);

    String rawValue = params[0].trim();
    Preconditions.checkArgument(
        !rawValue.isEmpty(),
        "Invalid SET metadata '%s' for SET index '%s': set_max_values is required",
        typeFull,
        indexName);
    Preconditions.checkArgument(
        SET_MAX_VALUES_PATTERN.matcher(rawValue).matches(),
        "Invalid SET metadata '%s' for SET index '%s': set_max_values '%s' is not a valid decimal integer",
        typeFull,
        indexName,
        rawValue);
    BigInteger value;
    try {
      value = new BigInteger(rawValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid SET metadata '%s' for SET index '%s': set_max_values '%s' is not a valid decimal integer"
              .formatted(typeFull, indexName, rawValue),
          e);
    }

    if (value.compareTo(MIN_SET_MAX_VALUES) < 0 || value.compareTo(MAX_SET_MAX_VALUES) > 0) {
      throw new IllegalArgumentException(
          "Invalid SET metadata '%s' for SET index '%s': set_max_values '%s' is outside supported range %s"
              .formatted(typeFull, indexName, rawValue, SET_MAX_VALUES_RANGE));
    }
    if (value.equals(MIN_SET_MAX_VALUES)) {
      return Collections.emptyMap();
    }
    return Map.of(SET_MAX_VALUES, value.toString());
  }

  private static Map<String, String> parseIndexPropertiesForQuery(
      Index.IndexType indexType,
      String parameterSource,
      String indexName,
      boolean allowBareLegacyType) {
    switch (indexType) {
      case DATA_SKIPPING_SET:
        return parseSetPropertiesForQuery(
            indexType, parameterSource, indexName, allowBareLegacyType);
      case DATA_SKIPPING_NGRAMBFV1:
      case DATA_SKIPPING_TOKENBFV1:
        return parseBloomFilterPropertiesForQuery(
            indexType, parameterSource, indexName, allowBareLegacyType);
      default:
        return Collections.emptyMap();
    }
  }

  private static Map<String, String> parseSetPropertiesForQuery(
      Index.IndexType indexType,
      String parameterSource,
      String indexName,
      boolean allowBareLegacyType) {
    if (allowBareLegacyType && !StringUtils.contains(parameterSource, "(")) {
      return Collections.emptyMap();
    }
    return parseSetProperties(indexType, parameterSource, indexName);
  }

  /**
   * Parses the positional parameters returned by ClickHouse in {@code type_full} for the two
   * parameterized bloom-filter data skipping indexes.
   *
   * @param indexType the mapped Gravitino index type
   * @param typeFull the complete ClickHouse index type expression
   * @param indexName the index name for validation messages
   * @return the index properties, or an empty map for non-parameterized index types
   * @throws IllegalArgumentException if a supported index has malformed or invalid parameters
   */
  @VisibleForTesting
  static Map<String, String> parseBloomFilterProperties(
      Index.IndexType indexType, String typeFull, String indexName) {
    if (!isParameterizedBloomFilterIndex(indexType)) {
      return Collections.emptyMap();
    }

    String expectedType =
        indexType == Index.IndexType.DATA_SKIPPING_NGRAMBFV1
            ? DATA_SKIPPING_NGRAMBFV1
            : DATA_SKIPPING_TOKENBFV1;
    String normalizedTypeFull = StringUtils.trimToEmpty(typeFull);
    int paramsStart = normalizedTypeFull.indexOf('(');
    int paramsEnd = normalizedTypeFull.lastIndexOf(')');
    Preconditions.checkArgument(
        paramsStart > 0 && paramsEnd == normalizedTypeFull.length() - 1,
        "Invalid type_full '%s' for %s index '%s'",
        typeFull,
        expectedType,
        indexName);
    Preconditions.checkArgument(
        StringUtils.equalsIgnoreCase(
            expectedType, normalizedTypeFull.substring(0, paramsStart).trim()),
        "type_full '%s' does not match %s index '%s'",
        typeFull,
        expectedType,
        indexName);

    String[] params = normalizedTypeFull.substring(paramsStart + 1, paramsEnd).split(",", -1);
    int expectedParamCount = indexType == Index.IndexType.DATA_SKIPPING_NGRAMBFV1 ? 4 : 3;
    Preconditions.checkArgument(
        params.length == expectedParamCount,
        "Expected %s parameters for %s index '%s', but got %s in '%s'",
        expectedParamCount,
        expectedType,
        indexName,
        params.length,
        typeFull);

    Map<String, String> properties = new HashMap<>();
    int paramIndex = 0;
    if (indexType == Index.IndexType.DATA_SKIPPING_NGRAMBFV1) {
      properties.put(
          NGRAM_SIZE,
          requireIntWithMin(params[paramIndex++], NGRAM_SIZE, expectedType, indexName, 1));
    }
    properties.put(
        BLOOM_FILTER_SIZE,
        requireIntWithMin(params[paramIndex++], BLOOM_FILTER_SIZE, expectedType, indexName, 1));
    properties.put(
        HASH_FUNCTIONS,
        requireIntWithMin(params[paramIndex++], HASH_FUNCTIONS, expectedType, indexName, 1));
    properties.put(
        RANDOM_SEED,
        requireIntWithMin(params[paramIndex], RANDOM_SEED, expectedType, indexName, 0));
    return Map.copyOf(properties);
  }

  private static Map<String, String> parseBloomFilterPropertiesForQuery(
      Index.IndexType indexType,
      String parameterSource,
      String indexName,
      boolean allowBareLegacyType) {
    if (!isParameterizedBloomFilterIndex(indexType)) {
      return Collections.emptyMap();
    }
    if (allowBareLegacyType && !StringUtils.contains(parameterSource, "(")) {
      return Collections.emptyMap();
    }
    return parseBloomFilterProperties(indexType, parameterSource, indexName);
  }

  private static boolean isParameterizedBloomFilterIndex(Index.IndexType indexType) {
    return indexType == Index.IndexType.DATA_SKIPPING_NGRAMBFV1
        || indexType == Index.IndexType.DATA_SKIPPING_TOKENBFV1;
  }

  /**
   * Maps a ClickHouse data skipping index type string to the corresponding Gravitino {@link
   * Index.IndexType}. Returns {@code DATA_SKIPPING_MINMAX} for blank/null input (ClickHouse
   * default). Also handles the {@code set(N)} parameterized format that some ClickHouse versions
   * may return from {@code system.data_skipping_indices}.
   *
   * @param rawType the index type string from ClickHouse metadata (e.g. "minmax", "bloom_filter",
   *     "set", "set(0)")
   * @return the corresponding Gravitino IndexType
   * @throws IllegalArgumentException if the type is not supported
   */
  @VisibleForTesting
  Index.IndexType getClickHouseIndexType(String rawType) {
    if (StringUtils.isBlank(rawType)) {
      return Index.IndexType.DATA_SKIPPING_MINMAX;
    }

    switch (rawType) {
      case DATA_SKIPPING_MINMAX_VALUE:
        return Index.IndexType.DATA_SKIPPING_MINMAX;
      case DATA_SKIPPING_BLOOM_FILTER:
        return Index.IndexType.DATA_SKIPPING_BLOOM_FILTER;
      case DATA_SKIPPING_SET:
        return Index.IndexType.DATA_SKIPPING_SET;
      case DATA_SKIPPING_NGRAMBFV1:
        return Index.IndexType.DATA_SKIPPING_NGRAMBFV1;
      case DATA_SKIPPING_TOKENBFV1:
        return Index.IndexType.DATA_SKIPPING_TOKENBFV1;
      default:
        // ClickHouse may return type with parameters in some versions (e.g. "set(0)",
        // "ngrambf_v1(3, 512, 3, 0)"). Match on prefix to handle both bare and
        // parameterized formats.
        if (rawType.startsWith(DATA_SKIPPING_SET + "(")) {
          return Index.IndexType.DATA_SKIPPING_SET;
        }
        if (rawType.startsWith(DATA_SKIPPING_NGRAMBFV1 + "(")) {
          return Index.IndexType.DATA_SKIPPING_NGRAMBFV1;
        }
        if (rawType.startsWith(DATA_SKIPPING_TOKENBFV1 + "(")) {
          return Index.IndexType.DATA_SKIPPING_TOKENBFV1;
        }
        throw new IllegalArgumentException("Unsupported data skipping index type: " + rawType);
    }
  }

  /**
   * Validates that a property value is an integer with a given minimum bound, returning it as a
   * string for DDL interpolation. Unlike {@link #resolveIntProperty(Map, String, int, int)}, this
   * method treats the value as required — a missing or blank value throws {@link
   * IllegalArgumentException} rather than returning a default. Used for bloom-filter parameters
   * (e.g. {@code bloom_filter_size}, {@code ngram_size} require &ge; 1; {@code random_seed}
   * requires &ge; 0).
   *
   * @param value the raw string value from the properties map
   * @param paramName the parameter name for error messages
   * @param indexType the index type name (e.g. "ngrambf_v1")
   * @param indexName the index name for error messages
   * @param minValue the minimum allowed value (inclusive)
   * @return the validated value as a string
   * @throws IllegalArgumentException if the value is null, blank, not an integer, or below minValue
   */
  private static String requireIntWithMin(
      String value, String paramName, String indexType, String indexName, int minValue) {
    Preconditions.checkArgument(
        value != null && !value.isBlank(),
        "%s is required for %s index '%s'",
        paramName,
        indexType,
        indexName);
    try {
      int intVal = Integer.parseInt(value.strip());
      Preconditions.checkArgument(
          intVal >= minValue,
          "%s must be >= %s for %s index '%s', but got '%s'",
          paramName,
          minValue,
          indexType,
          indexName,
          value);
      return String.valueOf(intVal);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "%s must be a valid integer for %s index '%s', but got '%s'",
              paramName, indexType, indexName, value),
          e);
    }
  }

  /**
   * Builds the full type clause for bloom-filter-based data skipping indexes, combining the type
   * name (e.g. "ngrambf_v1") with the validated parameter clause. Extracted to eliminate
   * duplication between the CREATE TABLE path ({@link #appendIndexesSql}) and the ALTER TABLE ADD
   * INDEX path ({@link #addIndexDefinition}).
   *
   * @param props the index properties map
   * @param indexType the index type name (one of the {@code DATA_SKIPPING_*} constants)
   * @param indexName the index name, used in error messages
   * @return the full type clause, e.g. "ngrambf_v1(3, 512, 3, 0)"
   */
  private static String buildBloomFilterTypeClause(
      Map<String, String> props, String indexType, String indexName) {
    return indexType + resolveBloomFilterParams(props, indexType, indexName);
  }

  /**
   * Resolves bloom-filter-based data skipping index parameters from index properties for {@code
   * ngrambf_v1} and {@code tokenbf_v1} index types. Used by both CREATE TABLE (via {@link
   * Index#properties()}) and ALTER TABLE ADD INDEX (via {@link
   * TableChange.AddIndex#getProperties()}).
   *
   * @param props the index properties map
   * @param indexType "ngrambf_v1" or "tokenbf_v1" (determines whether ngram_size is required)
   * @param indexName the index name, used in error messages
   * @return the DDL parameter clause, e.g. "(3, 512, 3, 0)" for ngrambf_v1
   * @throws IllegalArgumentException if any required parameter is missing or invalid
   */
  private static String resolveBloomFilterParams(
      Map<String, String> props, String indexType, String indexName) {
    String size =
        requireIntWithMin(
            props.get(BLOOM_FILTER_SIZE), "bloom_filter_size", indexType, indexName, 1);
    String hashFuncs =
        requireIntWithMin(props.get(HASH_FUNCTIONS), "hash_functions", indexType, indexName, 1);
    String seed = requireIntWithMin(props.get(RANDOM_SEED), "random_seed", indexType, indexName, 0);

    if (DATA_SKIPPING_NGRAMBFV1.equals(indexType)) {
      String ngramSize =
          requireIntWithMin(props.get(NGRAM_SIZE), "ngram_size", indexType, indexName, 1);
      return String.format("(%s, %s, %s, %s)", ngramSize, size, hashFuncs, seed);
    }
    return String.format("(%s, %s, %s)", size, hashFuncs, seed);
  }

  private String buildDataSkippingIndexDdl(
      String indexName,
      String fieldStr,
      Index.IndexType indexType,
      Map<String, String> properties) {
    return buildDataSkippingIndexDdl(
        indexName,
        fieldStr,
        resolveDataSkippingIndexTypeClause(indexType, properties, indexName),
        resolveGranularity(properties, 1));
  }

  private String resolveDataSkippingIndexTypeClause(
      Index.IndexType indexType, Map<String, String> properties, String indexName) {
    switch (indexType) {
      case DATA_SKIPPING_MINMAX:
        return DATA_SKIPPING_MINMAX_VALUE;
      case DATA_SKIPPING_BLOOM_FILTER:
        return DATA_SKIPPING_BLOOM_FILTER;
      case DATA_SKIPPING_SET:
        // SET index defaults to unlimited distinct values and supports an optional upper bound.
        return "set(" + resolveSetMaxValues(properties) + ")";
      case DATA_SKIPPING_NGRAMBFV1:
        return buildBloomFilterTypeClause(properties, DATA_SKIPPING_NGRAMBFV1, indexName);
      case DATA_SKIPPING_TOKENBFV1:
        return buildBloomFilterTypeClause(properties, DATA_SKIPPING_TOKENBFV1, indexName);
      default:
        throw new IllegalArgumentException(
            "Gravitino ClickHouse doesn't support index : " + indexType);
    }
  }

  private String buildDataSkippingIndexDdl(
      String indexName, String fieldStr, String typeName, int granularity) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(indexName), "Data skipping index name must not be blank");
    return "INDEX %s %s TYPE %s GRANULARITY %d"
        .formatted(quoteIdentifier(indexName), fieldStr, typeName, granularity);
  }

  /**
   * Extracts engine parameters from the {@code engine_full} column of {@code system.tables}.
   *
   * <p>Matches the outer parentheses while ignoring parentheses inside quoted strings and
   * identifiers. For example, {@code SummingMergeTree((a, b))} returns {@code "(a, b)"}, and {@code
   * ReplacingMergeTree(`ver)`)} returns {@code "`ver)`"}. Engines without parameters return {@code
   * null}.
   */
  @VisibleForTesting
  @Nullable
  static String extractEngineParams(@Nullable String engineName, @Nullable String engineFull) {
    if (StringUtils.isBlank(engineFull) || StringUtils.isBlank(engineName)) {
      return null;
    }

    String normalizedEngineName = StringUtils.trim(engineName);
    String normalizedEngineFull = StringUtils.trim(engineFull);
    if (!StringUtils.startsWithIgnoreCase(normalizedEngineFull, normalizedEngineName)) {
      return null;
    }

    int paramsStart = normalizedEngineName.length();
    while (paramsStart < normalizedEngineFull.length()
        && Character.isWhitespace(normalizedEngineFull.charAt(paramsStart))) {
      paramsStart++;
    }
    if (paramsStart >= normalizedEngineFull.length()
        || normalizedEngineFull.charAt(paramsStart) != '(') {
      return null;
    }

    int paramsEnd = findMatchingParenthesis(normalizedEngineFull, paramsStart);
    if (paramsEnd < 0) {
      return null;
    }
    return normalizedEngineFull.substring(paramsStart + 1, paramsEnd).trim();
  }

  private static void validateEngineParameters(ENGINE engine, @Nullable String engineParams) {
    if (StringUtils.isBlank(engineParams)) {
      return;
    }

    if (engine == ENGINE.GRAPHITEMERGETREE) {
      throw new IllegalArgumentException(
          "'engine_parameters' is not supported for GraphiteMergeTree; use 'graphite.config'");
    }
    if (engine == ENGINE.DISTRIBUTED) {
      throw new IllegalArgumentException(
          "'engine_parameters' is not supported for Distributed; use the distributed table "
              + "properties");
    }
    Preconditions.checkArgument(
        GENERIC_ENGINE_PARAMETER_ENGINES.contains(engine),
        "'engine_parameters' is not supported for ClickHouse engine %s",
        engine.getValue());

    String wrappedParams = "(" + engineParams + ")";
    Preconditions.checkArgument(
        findMatchingParenthesis(wrappedParams, 0) == wrappedParams.length() - 1,
        "Invalid 'engine_parameters' for ClickHouse engine %s: parentheses and quotes must be "
            + "balanced",
        engine.getValue());
  }

  private static boolean isGenericEngineParameterEngine(@Nullable String engineName) {
    return GENERIC_ENGINE_PARAMETER_ENGINES.stream()
        .anyMatch(engine -> StringUtils.equalsIgnoreCase(engine.getValue(), engineName));
  }

  @Nullable
  private static String extractGraphiteConfig(@Nullable String engineFull) {
    String engineParams = extractEngineParams(ENGINE.GRAPHITEMERGETREE.getValue(), engineFull);
    if (!isSingleQuotedLiteral(engineParams)) {
      return null;
    }

    String quotedConfig = StringUtils.trim(engineParams);
    return JdbcConnectorUtils.unescapeSqlLiteral(
        quotedConfig.substring(1, quotedConfig.length() - 1), '\'');
  }

  private static boolean isSingleQuotedLiteral(@Nullable String value) {
    String literal = StringUtils.trim(value);
    if (StringUtils.length(literal) < 2 || literal.charAt(0) != '\'') {
      return false;
    }

    for (int i = 1; i < literal.length(); i++) {
      char current = literal.charAt(i);
      if (current == '\\') {
        if (i + 1 >= literal.length()) {
          return false;
        }
        i++;
      } else if (current == '\'') {
        if (i + 1 < literal.length() && literal.charAt(i + 1) == '\'') {
          i++;
        } else {
          return i == literal.length() - 1;
        }
      }
    }
    return false;
  }

  private static int findMatchingParenthesis(String value, int openParenthesis) {
    int depth = 1;
    char quote = 0;
    for (int i = openParenthesis + 1; i < value.length(); i++) {
      char current = value.charAt(i);
      if (quote != 0) {
        if (current == '\\' && i + 1 < value.length()) {
          i++;
        } else if (current == quote) {
          if (i + 1 < value.length() && value.charAt(i + 1) == quote) {
            i++;
          } else {
            quote = 0;
          }
        }
        continue;
      }

      if (current == '\'' || current == '"' || current == '`') {
        quote = current;
      } else if (current == '(') {
        depth++;
      } else if (current == ')') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return -1;
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
