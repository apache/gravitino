/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.google.common.collect.ImmutableList;
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
    String sql = String.format("SHOW PARTITIONS FROM %s", tableName);
    try (ResultSet result = getConnection(databaseName).createStatement().executeQuery(sql)) {
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

    return new Partition[0];
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
}
