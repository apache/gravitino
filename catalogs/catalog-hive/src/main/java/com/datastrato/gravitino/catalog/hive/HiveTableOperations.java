/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.catalog.TableOperations;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;

public class HiveTableOperations implements TableOperations, SupportsPartitions {

  private static final String PARTITION_NAME_DELIMITER = "/";
  private static final String PARTITION_VALUE_DELIMITER = "=";

  private final HiveTable table;

  public HiveTableOperations(HiveTable table) {
    Preconditions.checkArgument(table != null, "table must not be null");
    this.table = table;
  }

  @Override
  public String[] listPartitionNames() {
    try {
      return table
          .clientPool()
          .run(
              c ->
                  c.listPartitionNames(table.schemaName(), table.name(), (short) -1)
                      .toArray(new String[0]));
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to list partition names of table " + table.name() + "from Hive Metastore", e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    try {
      org.apache.hadoop.hive.metastore.api.Partition partition =
          table
              .clientPool()
              .run(c -> c.getPartition(table.schemaName(), table.name(), partitionName));
      return fromHivePartition(partitionName, partition);

    } catch (UnknownTableException e) {
      throw new NoSuchTableException(
          "Hive table " + table.name() + " does not exist in Hive Metastore", e);

    } catch (NoSuchObjectException e) {
      throw new NoSuchPartitionException(
          "Hive partition " + partitionName + " does not exist in Hive Metastore", e);

    } catch (TException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to get partition "
              + partitionName
              + " of table "
              + table.name()
              + "from Hive Metastore",
          e);
    }
  }

  private Partition fromHivePartition(
      String partitionName, org.apache.hadoop.hive.metastore.api.Partition partition) {
    String[][] fieldNames = getFieldNames(partitionName);
    Literal[] values =
        partition.getValues().stream().map(Literals::stringLiteral).toArray(Literal[]::new);
    // todo: support partition properties metadata to get more necessary information
    return Partitions.identity(partitionName, fieldNames, values, partition.getParameters());
  }

  private String[][] getFieldNames(String partitionName) {
    // Hive partition name is in the format of "field1=value1/field2=value2/..."
    String[] fields = partitionName.split(PARTITION_NAME_DELIMITER);
    return Arrays.stream(fields)
        .map(field -> new String[] {field.split(PARTITION_VALUE_DELIMITER)[0]})
        .toArray(String[][]::new);
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String partitionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    table.close();
  }
}
