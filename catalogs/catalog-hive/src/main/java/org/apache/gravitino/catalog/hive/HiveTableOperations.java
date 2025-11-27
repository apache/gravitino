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

import static org.apache.gravitino.hive.HivePartition.extractPartitionValues;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableOperations implements TableOperations, SupportsPartitions {
  public static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private static final String PARTITION_NAME_DELIMITER = "/";
  private static final String PARTITION_VALUE_DELIMITER = "=";

  private final HiveTableHandle tableHandle;

  public HiveTableOperations(HiveTableHandle tableHandle) {
    Preconditions.checkArgument(tableHandle != null, "table must not be null");
    this.tableHandle = tableHandle;
  }

  @Override
  public String[] listPartitionNames() {
    try {
      return tableHandle
          .clientPool()
          .run(c -> c.listPartitionNames(tableHandle.table(), (short) -1).toArray(new String[0]));
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Failed to list partition names of table " + tableHandle.name() + "from Hive Metastore",
          e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    try {
      return tableHandle
          .clientPool()
          .run(c -> c.listPartitions(tableHandle.table(), (short) -1))
          .toArray(new Partition[0]);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Failed to list partitions of table " + tableHandle.name() + "from Hive Metastore", e);
    }
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    try {
      return tableHandle.clientPool().run(c -> c.getPartition(tableHandle.table(), partitionName));

    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Failed to get partition "
              + partitionName
              + " of table "
              + tableHandle.name()
              + "from Hive Metastore",
          e);
    }
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    if (MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(partition.name())) {
      throw new IllegalArgumentException("Can't create a catalog with with reserved partition `*`");
    }

    Preconditions.checkArgument(
        partition instanceof IdentityPartition, "Hive only supports identity partition");
    IdentityPartition identityPartition = (IdentityPartition) partition;
    HivePartition hivePartition =
        HivePartition.identity(
            identityPartition.fieldNames(),
            identityPartition.values(),
            identityPartition.properties());

    Set<String> partitionFieldNames = new HashSet<>(tableHandle.table().partitionFieldNames());
    Preconditions.checkArgument(
        partitionFieldNames.size() == identityPartition.fieldNames().length,
        "Hive partition field names must be the same as table partitioning field names: %s, but got %s",
        String.join(",", partitionFieldNames),
        String.join(
            ",",
            Arrays.stream(identityPartition.fieldNames())
                .map(f -> String.join(".", f))
                .collect(Collectors.toList())));
    Arrays.stream(identityPartition.fieldNames())
        .forEach(
            f ->
                Preconditions.checkArgument(
                    partitionFieldNames.contains(f[0]),
                    "Hive partition field name must be in table partitioning field names: %s, but got %s",
                    String.join(",", partitionFieldNames),
                    f[0]));

    try {
      return tableHandle.clientPool().run(c -> c.addPartition(tableHandle.table(), hivePartition));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean dropPartition(String partitionName) {
    try {
      HiveTable hiveTable = tableHandle.table();
      // Get partitions that need to drop
      // If the partition has child partition, then drop all the child partitions
      // If the partition has no subpartitions, then just drop the partition
      List<HivePartition> partitions =
          tableHandle
              .clientPool()
              .run(
                  c ->
                      c.listPartitions(
                          hiveTable,
                          extractPartitionValues(hiveTable.partitionFieldNames(), partitionName),
                          (short) -1));
      if (partitions.isEmpty()) {
        throw new NoSuchPartitionException(
            "Hive partition %s does not exist in Hive Metastore", partitionName);
      }
      // Delete partitions iteratively
      for (HivePartition partition : partitions) {
        tableHandle
            .clientPool()
            .run(
                cc -> {
                  cc.dropPartition(
                      hiveTable.catalogName(),
                      hiveTable.databaseName(),
                      tableHandle.name(),
                      partition.name(),
                      true);
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
              + tableHandle.name()
              + "from Hive Metastore",
          e);
    }
    return true;
  }

  private boolean validPartitionName(String partitionName) {
    if (StringUtils.isEmpty(partitionName)) {
      return false;
    }

    List<String> partitionFields = tableHandle.table().partitionFieldNames();
    String[] parts = partitionName.split(PARTITION_NAME_DELIMITER);
    if (parts.length != partitionFields.size()) {
      return false;
    }

    for (int i = 0; i < parts.length; i++) {
      String[] keyValue = parts[i].split(PARTITION_VALUE_DELIMITER, 2);
      if (keyValue.length != 2 || !partitionFields.get(i).equals(keyValue[0])) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() throws IOException {
    tableHandle.close();
  }
}
