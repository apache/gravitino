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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.DATA_SIZE;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.ID;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.IS_IN_MEMORY;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.KEY;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.NAME;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.STATE;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.VALUES_RANGE;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.VISIBLE_VERSION;
import static org.apache.gravitino.catalog.doris.DorisTablePartitionPropertiesMetadata.VISIBLE_VERSION_TIME;
import static org.apache.gravitino.catalog.doris.utils.DorisUtils.generatePartitionSqlFragment;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Type;

public final class DorisTablePartitionOperations extends JdbcTablePartitionOperations {
  private static final String PARTITION_TYPE_VALUE_PATTERN_STRING =
      "types: \\[([^\\]]+)\\]; keys: \\[([^\\]]+)\\];";
  private static final Pattern PARTITION_TYPE_VALUE_PATTERN =
      Pattern.compile(PARTITION_TYPE_VALUE_PATTERN_STRING);

  private final JdbcExceptionConverter exceptionConverter;
  private final JdbcTypeConverter typeConverter;

  public DorisTablePartitionOperations(
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
          partitions.add(fromDorisPartition(result, partitionInfo, columnTypes));
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
          return fromDorisPartition(result, partitionInfo, columnTypes);
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
        partitionSqlFragment = generatePartitionSqlFragment(rangePartition);

        // The partition properties actually cannot be passed into Doris, we just return an empty
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

        partitionSqlFragment = generatePartitionSqlFragment(listPartition);

        added =
            Partitions.list(listPartition.name(), listPartition.lists(), Collections.emptyMap());
      } else {
        throw new IllegalArgumentException("Unsupported partition type of Doris");
      }

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

  private Partition fromDorisPartition(
      ResultSet resultSet, Transform partitionInfo, Map<String, Type> columnTypes)
      throws SQLException {
    String partitionName = resultSet.getString(NAME);
    String partitionKey = resultSet.getString(KEY);
    String partitionValues = resultSet.getString(VALUES_RANGE);
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.put(ID, resultSet.getString(ID));
    propertiesBuilder.put(VISIBLE_VERSION, resultSet.getString(VISIBLE_VERSION));
    propertiesBuilder.put(VISIBLE_VERSION_TIME, resultSet.getString(VISIBLE_VERSION_TIME));
    propertiesBuilder.put(STATE, resultSet.getString(STATE));
    propertiesBuilder.put(KEY, partitionKey);
    propertiesBuilder.put(DATA_SIZE, resultSet.getString(DATA_SIZE));
    propertiesBuilder.put(IS_IN_MEMORY, resultSet.getString(IS_IN_MEMORY));
    ImmutableMap<String, String> properties = propertiesBuilder.build();

    String[] partitionKeys = partitionKey.split(", ");
    if (partitionInfo instanceof Transforms.RangeTransform) {
      if (partitionKeys.length != 1) {
        throw new UnsupportedOperationException(
            "Multi-column range partitioning in Doris is not supported yet");
      }
      Type partitionColumnType = columnTypes.get(partitionKeys[0]);
      Literal<?> lower = Literals.NULL;
      Literal<?> upper = Literals.NULL;
      Matcher matcher = PARTITION_TYPE_VALUE_PATTERN.matcher(partitionValues);
      if (matcher.find()) {
        String lowerValue = matcher.group(2);
        lower = Literals.of(lowerValue, partitionColumnType);
        if (matcher.find()) {
          String upperValue = matcher.group(2);
          upper = Literals.of(upperValue, partitionColumnType);
        }
      }
      return Partitions.range(partitionName, upper, lower, properties);
    } else if (partitionInfo instanceof Transforms.ListTransform) {
      Matcher matcher = PARTITION_TYPE_VALUE_PATTERN.matcher(partitionValues);
      ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
      while (matcher.find()) {
        String[] values = matcher.group(2).split(", ");
        ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
        for (int i = 0; i < values.length; i++) {
          Type partitionColumnType = columnTypes.get(partitionKeys[i]);
          literValues.add(Literals.of(values[i], partitionColumnType));
        }
        lists.add(literValues.build().toArray(new Literal<?>[0]));
      }
      return Partitions.list(
          partitionName, lists.build().toArray(new Literal<?>[0][0]), properties);
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not a partitioned table", loadedTable.name()));
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
          typeBean.setColumnSize(result.getString("COLUMN_SIZE"));
          typeBean.setScale(result.getString("DECIMAL_DIGITS"));
          Type gravitinoType = typeConverter.toGravitino(typeBean);
          String columnName = result.getString("COLUMN_NAME");
          columnTypes.put(columnName, gravitinoType);
        }
      }
      return columnTypes.build();
    }
  }
}
