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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.catalog.starrocks.utils.StarRocksUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Table partition operations for StarRocks. */
public final class StarRocksTablePartitionOperations extends JdbcTablePartitionOperations {

  private static final Logger log =
      LoggerFactory.getLogger(StarRocksTablePartitionOperations.class);

  private final JdbcExceptionConverter exceptionConverter;

  private final JdbcTypeConverter typeConverter;

  public StarRocksTablePartitionOperations(
      DataSource dataSource,
      JdbcTable loadedTable,
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter typeConverter) {
    super(dataSource, loadedTable);
    checkArgument(exceptionConverter != null, "exceptionConverter is null");
    checkArgument(typeConverter != null, "typeConverter is null");
    this.exceptionConverter = exceptionConverter;
    this.typeConverter = typeConverter;
  }

  @Override
  public String[] listPartitionNames() {
    try (Connection connection = getConnection(loadedTable.databaseName())) {
      String showPartitionsSql = String.format("SHOW PARTITIONS FROM `%s`", loadedTable.name());
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(showPartitionsSql)) {
        ImmutableList.Builder<String> partitionNames = ImmutableList.builder();
        while (result.next()) {
          partitionNames.add(result.getString("PartitionName"));
        }
        return partitionNames.build().toArray(new String[0]);
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    try (Connection connection = getConnection(loadedTable.databaseName())) {
      Transform partitionInfo = loadedTable.partitioning()[0];
      Map<String, Type> columnTypes = getColumnType(connection);
      String showPartitionsSql = String.format("SHOW PARTITIONS FROM `%s`", loadedTable.name());
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(showPartitionsSql)) {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        while (result.next()) {
          partitions.add(
              StarRocksUtils.fromStarRocksPartition(
                  loadedTable.name(), result, partitionInfo, columnTypes));
        }
        return partitions.build().toArray(new Partition[0]);
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    try (Connection connection = getConnection(loadedTable.databaseName())) {
      Transform partitionInfo = loadedTable.partitioning()[0];
      Map<String, Type> columnTypes = getColumnType(connection);
      String showPartitionsSql =
          String.format(
              "SHOW PARTITIONS FROM `%s` WHERE PartitionName = \"%s\"",
              loadedTable.name(), partitionName);
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(showPartitionsSql)) {
        if (result.next()) {
          return StarRocksUtils.fromStarRocksPartition(
              loadedTable.name(), result, partitionInfo, columnTypes);
        }
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
    throw new NoSuchPartitionException("Partition %s does not exist", partitionName);
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    try (Connection connection = getConnection(loadedTable.databaseName())) {
      Transform partitionInfo = loadedTable.partitioning()[0];

      String addPartitionSqlFormat = "ALTER TABLE `%s` ADD %s";
      String partitionSqlFragment;
      Partition added;

      if (partition instanceof RangePartition) {
        Preconditions.checkArgument(
            partitionInfo instanceof Transforms.RangeTransform,
            "Table %s is non-range-partitioned, but trying to add a range partition",
            loadedTable.name());

        RangePartition rangePartition = (RangePartition) partition;
        partitionSqlFragment = StarRocksUtils.generatePartitionSqlFragment(rangePartition);

        // The partition properties actually cannot be passed into StarRocks, we just return an
        // empty
        // map instead.
        added =
            Partitions.range(
                rangePartition.name(),
                rangePartition.upper(),
                rangePartition.lower(),
                Collections.emptyMap());
      } else if (partition instanceof ListPartition) {
        Preconditions.checkArgument(
            partitionInfo instanceof Transforms.ListTransform,
            "Table %s is non-list-partitioned, but trying to add a list partition",
            loadedTable.name());

        ListPartition listPartition = (ListPartition) partition;
        Literal<?>[][] lists = listPartition.lists();
        Preconditions.checkArgument(
            lists.length > 0, "The number of values in list partition must be greater than 0");
        Preconditions.checkArgument(
            Arrays.stream(lists)
                .allMatch(
                    part ->
                        part.length
                            == ((Transforms.ListTransform) partitionInfo).fieldNames().length),
            "The number of partitioning columns must be consistent");

        partitionSqlFragment = StarRocksUtils.generatePartitionSqlFragment(listPartition);

        added =
            Partitions.list(listPartition.name(), listPartition.lists(), Collections.emptyMap());
      } else {
        throw new IllegalArgumentException("Unsupported partition type of StarRocks");
      }
      log.info("Generated add partition sql : {}", partitionSqlFragment);
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(
            String.format(addPartitionSqlFormat, loadedTable.name(), partitionSqlFragment));
        return added;
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public boolean dropPartition(String partitionName) {
    try (Connection connection = getConnection(loadedTable.databaseName())) {
      String dropPartitionsSql =
          String.format("ALTER TABLE `%s` DROP PARTITION `%s`", loadedTable.name(), partitionName);
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(dropPartitionsSql);
        return true;
      }
    } catch (SQLException e) {
      GravitinoRuntimeException exception = exceptionConverter.toGravitinoException(e);
      if (exception instanceof NoSuchPartitionException) {
        return false;
      }
      throw exception;
    }
  }

  private Map<String, Type> getColumnType(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet result =
        metaData.getColumns(
            connection.getCatalog(), connection.getSchema(), loadedTable.name(), null)) {
      ImmutableMap.Builder<String, Type> columnTypes = ImmutableMap.builder();
      while (result.next()) {
        if (Objects.equals(result.getString("TABLE_NAME"), loadedTable.name())) {
          JdbcTypeConverter.JdbcTypeBean typeBean =
              new JdbcTypeConverter.JdbcTypeBean(result.getString("TYPE_NAME"));
          typeBean.setColumnSize(result.getInt("COLUMN_SIZE"));
          typeBean.setScale(result.getInt("DECIMAL_DIGITS"));
          Type gravitinoType = typeConverter.toGravitino(typeBean);
          String columnName = result.getString("COLUMN_NAME");
          columnTypes.put(columnName, gravitinoType);
        }
      }
      return columnTypes.build();
    }
  }
}
