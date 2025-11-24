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
package org.apache.gravitino.catalog.hive;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableOperations implements TableOperations, SupportsPartitions {
  public static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

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
                  c.listPartitionNames("", table.schemaName(), table.name(), (short) -1)
                      .toArray(new String[0]));
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Failed to list partition names of table " + table.name() + "from Hive Metastore", e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    try {
      return table
          .clientPool()
          .run(c -> c.listPartitions("", table.schemaName(), table, (short) -1))
          .toArray(new Partition[0]);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    try {
      return table
          .clientPool()
          .run(c -> c.getPartition("", table.schemaName(), table.name(), partitionName));

    } catch (InterruptedException e) {
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
    if (MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(partition.name())) {
      throw new IllegalArgumentException("Can't create a catalog with with reserved partition `*`");
    }

    Preconditions.checkArgument(
        partition instanceof IdentityPartition, "Hive only supports identity partition");
    IdentityPartition identityPartition = (IdentityPartition) partition;

    Set<String> transformFields =
        Arrays.stream(table.partitioning())
            .map(t -> ((Transforms.IdentityTransform) t).fieldName()[0])
            .collect(Collectors.toSet());

    Preconditions.checkArgument(
        transformFields.size() == identityPartition.fieldNames().length,
        "Hive partition field names must be the same as table partitioning field names: %s, but got %s",
        String.join(",", transformFields),
        String.join(
            ",",
            Arrays.stream(identityPartition.fieldNames())
                .map(f -> String.join(".", f))
                .collect(Collectors.toList())));
    Arrays.stream(identityPartition.fieldNames())
        .forEach(
            f ->
                Preconditions.checkArgument(
                    transformFields.contains(f[0]),
                    "Hive partition field name must be in table partitioning field names: %s, but got %s",
                    String.join(",", transformFields),
                    f[0]));

    try {
      return table.clientPool().run(c -> c.addPartition("", "", "", identityPartition));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private String generatePartitionName(IdentityPartition partition) {
    Arrays.stream(partition.fieldNames())
        .forEach(
            fieldName ->
                Preconditions.checkArgument(
                    fieldName.length == 1,
                    "Hive catalog does not support nested partition field names"));

    // Hive partition name is in the format of "field1=value1/field2=value2/..."
    return IntStream.range(0, partition.fieldNames().length)
        .mapToObj(
            i ->
                partition.fieldNames()[i][0]
                    + PARTITION_VALUE_DELIMITER
                    + partition.values()[i].value().toString())
        .collect(Collectors.joining(PARTITION_NAME_DELIMITER));
  }

  @Override
  public boolean dropPartition(String partitionName) {
    try {
      Table tb = table.clientPool().run(c -> c.getTable("", table.schemaName(), table.name()));
      HiveTable hiveTable = HiveTable.fromHiveTable(table.schemaName(), tb).build();
      // Get partitions that need to drop
      // If the partition has child partition, then drop all the child partitions
      // If the partition has no subpartitions, then just drop the partition
      List<Partition> partitions =
          table
              .clientPool()
              .run(
                  c ->
                      c.listPartitions(
                          "",
                          table.schemaName(),
                          table,
                          getFilterPartitionValueList(hiveTable, partitionName),
                          (short) -1));
      if (partitions.isEmpty()) {
        throw new NoSuchPartitionException(
            "Hive partition %s does not exist in Hive Metastore", partitionName);
      }
      // Delete partitions iteratively
      for (Partition partition : partitions) {
        table
            .clientPool()
            .run(
                cc -> {
                  cc.dropPartition("", table.schemaName(), table.name(), partition.name(), true);
                  return null;
                });
      }

    } catch (NoSuchPartitionException e) {
      return false;

    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Failed to get partition "
              + partitionName
              + " of table "
              + table.name()
              + "from Hive Metastore",
          e);
    }
    return true;
  }

  /**
   * Retrieve and complete partition field values from the given table and partitionSpec. The absent
   * partition values will be filled with empty string.
   *
   * <p>For example, if the table is partitioned by column "log_date", "log_hour", "log_minute", and
   * the partitionSpec is:
   *
   * <p>1) "log_date=2022-01-01/log_hour=01/log_minute=01", the return value should be
   * ["2022-01-01", "01", "01"].
   *
   * <p>2) "log_date=2022-01-01", the return value should be ["2022-01-01", "", ""].
   *
   * <p>3) "log_hour=01", the return value should be ["", "01", ""].
   *
   * @param dropTable the table to get partition fields from
   * @param partitionSpec partition in String format
   * @return the filter partition list
   * @throws IllegalArgumentException if the partitionSpec is not valid
   */
  private List<String> getFilterPartitionValueList(HiveTable dropTable, String partitionSpec)
      throws NoSuchPartitionException, IllegalArgumentException {
    // Get all partition key names of the table
    List<String> partitionKeys =
        dropTable.buildPartitionKeys().stream()
            .map(FieldSchema::getName)
            .collect(Collectors.toList());

    // Split and process the partition specification string
    Map<String, String> partSpecMap =
        Arrays.stream(partitionSpec.split(PARTITION_NAME_DELIMITER))
            .map(
                part -> {
                  String[] keyValue = part.split(PARTITION_VALUE_DELIMITER, 2);
                  if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Error partition format: " + partitionSpec);
                  }
                  if (!partitionKeys.contains(keyValue[0])) {
                    throw new NoSuchPartitionException(
                        "Hive partition %s does not exist in Hive Metastore", partitionSpec);
                  }
                  return keyValue;
                })
            .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));

    // Retrieve or populate partition values from partSpecMap based on the table's partition key
    // order
    List<String> partitionValues =
        partitionKeys.stream()
            .map(key -> partSpecMap.getOrDefault(key, ""))
            .collect(Collectors.toList());

    return partitionValues;
  }

  @Override
  public void close() throws IOException {
    table.close();
  }
}
