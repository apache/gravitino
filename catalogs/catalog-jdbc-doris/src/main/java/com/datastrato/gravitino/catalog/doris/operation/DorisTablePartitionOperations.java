/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.doris.utils.DorisUtils;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.NoSuchPartitionedTableException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

public final class DorisTablePartitionOperations extends JdbcTablePartitionOperations {

  public DorisTablePartitionOperations(
      JdbcExceptionConverter exceptionConverter,
      DataSource dataSource,
      String databaseName,
      String tableName) {
    super(exceptionConverter, dataSource, databaseName, tableName);
  }

  @Override
  public String[] listPartitionNames() {
    if (getPartitionType() == PartitionType.NONE) {
      throw new NoSuchPartitionedTableException("%s is not a partitioned table", tableName);
    }
    String showPartitions = String.format("SHOW PARTITIONS FROM %s", tableName);
    try (ResultSet result =
        getConnection(databaseName).createStatement().executeQuery(showPartitions)) {
      ImmutableList.Builder<String> partitionNames = ImmutableList.builder();
      while (result.next()) {
        partitionNames.add(result.getString("PartitionName"));
      }
      return partitionNames.build().toArray(new String[0]);
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    PartitionType partitionType = getPartitionType();
    if (partitionType == PartitionType.NONE) {
      throw new NoSuchPartitionedTableException("%s is not a partitioned table", tableName);
    }
    String showPartitions = String.format("SHOW PARTITIONS FROM %s", tableName);
    try (ResultSet result =
        getConnection(databaseName).createStatement().executeQuery(showPartitions)) {
      ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
      while (result.next()) {
        //        partitions.add(fromDorisPartition(result, partitionType));
      }
      return partitions.build().toArray(new Partition[0]);
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    return null;
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    return null;
  }

  @Override
  public boolean dropPartition(String partitionName) {
    return false;
  }

  private PartitionType getPartitionType() {
    String showCreateTable = String.format("SHOW CREATE TABLE %s", tableName);
    try (ResultSet result =
        getConnection(databaseName).createStatement().executeQuery(showCreateTable)) {
      StringBuilder createTableSql = new StringBuilder();
      while (result.next()) {
        createTableSql.append(result.getString("Create Table"));
      }
      return DorisUtils.extractPartitionTypeFromSql(createTableSql.toString());
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

    private Partition fromDorisPartition(ResultSet resultSet, PartitionType partitionType) throws
   SQLException {
      String partitionName = resultSet.getString("PartitionName");
      ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
      properties.put("PartitionId", resultSet.getString("PartitionId"));
      properties.put("State", resultSet.getString("State"));
      properties.put("PartitionKey", resultSet.getString("PartitionKey"));
      properties.put("DataSize", resultSet.getString("DataSize"));
      switch (partitionType) {
        case RANGE:
          String[] rangeStr = resultSet.getString("Range").split("\\.\\.");
          String[] lowerStr = rangeStr[0].split(";");

          return Partitions.range(partitionName, , , properties.build());
          break;
        case LIST:

          return Partitions.list(partitionName, , properties.build());
          break;
        default: throw new NoSuchPartitionedTableException("%s is not a partitioned table", tableName);
      }
    }

    private Literal partitionLiteral(String columnType, String value) {
      // 用column default converter？ 需要 column 信息
      Literals.varcharLiteral()
      return null;
    }

  public enum PartitionType {
    RANGE,
    LIST,
    NONE
  }
}
