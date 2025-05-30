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

package org.apache.gravitino.spark.connector.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

public class HiveGravitinoOperationOperator {

  private org.apache.gravitino.rel.Table gravitinoTable;
  private static final String PARTITION_NAME_DELIMITER = "/";
  private static final String PARTITION_VALUE_DELIMITER = "=";

  public HiveGravitinoOperationOperator(org.apache.gravitino.rel.Table gravitinoTable) {
    this.gravitinoTable = gravitinoTable;
  }

  public void createPartition(
      InternalRow ident, Map<String, String> properties, StructType partitionSchema)
      throws PartitionAlreadyExistsException {
    List<String[]> fields = new ArrayList<>();
    List<Literal<?>> values = new ArrayList<>();

    int numFields = ident.numFields();
    for (int i = 0; i < numFields; i++) {
      StructField structField = partitionSchema.apply(i);
      DataType dataType = structField.dataType();
      fields.add(new String[] {structField.name()});
      values.add(SparkPartitionUtils.toGravitinoLiteral(ident, i, dataType));
    }

    Partition partition =
        Partitions.identity(
            null, fields.toArray(new String[0][0]), values.toArray(new Literal[0]), properties);

    try {
      gravitinoTable.supportPartitions().addPartition(partition);
    } catch (org.apache.gravitino.exceptions.PartitionAlreadyExistsException e) {
      throw new org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException(
          e.getMessage());
    }
  }

  public boolean dropPartition(InternalRow ident, StructType partitionSchema) {
    String partitionName = getHivePartitionName(ident, partitionSchema);
    return gravitinoTable.supportPartitions().dropPartition(partitionName);
  }

  public InternalRow[] listPartitionIdentifiers(
      String[] names, InternalRow ident, StructType partitionSchema) {
    // Get all partitions
    String[] allPartitions = gravitinoTable.supportPartitions().listPartitionNames();

    boolean isNeedAll = names != null && names.length == 0 ? true : false;

    String[] partitionNames =
        getHivePartitionName(names, ident, partitionSchema).split(PARTITION_NAME_DELIMITER);
    String[] partitionNamesWithDelimiter =
        Arrays.stream(partitionNames)
            .map(name -> name + PARTITION_NAME_DELIMITER)
            .toArray(String[]::new);

    return Arrays.stream(allPartitions)
        .map(e -> e + PARTITION_NAME_DELIMITER)
        .filter(
            e -> {
              if (isNeedAll) {
                return true;
              }
              for (String name : partitionNamesWithDelimiter) {
                // exactly match
                if (!e.contains(name)) {
                  return false;
                }
              }
              return true;
            })
        .map(e -> e.substring(0, e.length() - 1))
        .map(e -> toSparkPartition(e, partitionSchema))
        .toArray(GenericInternalRow[]::new);
  }

  public Map<String, String> loadPartitionMetadata(InternalRow ident, StructType partitionSchema) {
    String partitionName = getHivePartitionName(ident, partitionSchema);
    Partition partition = gravitinoTable.supportPartitions().getPartition(partitionName);
    return partition == null ? Collections.emptyMap() : partition.properties();
  }

  public boolean partitionExists(String[] names, InternalRow ident, StructType partitionSchema) {
    // Get all partitions
    if (names != null && names.length == 0) {
      return gravitinoTable.supportPartitions().listPartitionNames().length > 0;
    }

    String partitionName = getHivePartitionName(names, ident, partitionSchema);
    try {
      return gravitinoTable.supportPartitions().partitionExists(partitionName);
    } catch (NoSuchPartitionException noSuchPartitionException) {
      return false;
    }
  }

  private InternalRow toSparkPartition(String partitionName, StructType partitionSchema) {
    String[] splits = partitionName.split(PARTITION_NAME_DELIMITER);
    Object[] values = new Object[splits.length];
    for (int i = 0; i < splits.length; i++) {
      values[i] =
          SparkPartitionUtils.getSparkPartitionValue(
              splits[i].split(PARTITION_VALUE_DELIMITER)[1], partitionSchema.apply(i).dataType());
    }
    return new GenericInternalRow(values);
  }

  private @NotNull String getHivePartitionName(
      String[] names, InternalRow ident, StructType partitionSchema) {
    StringBuilder partitionName = new StringBuilder();
    Preconditions.checkArgument(names != null, "Partition column names must not be null");
    for (int i = 0; i < names.length; i++) {
      StructField structField = partitionSchema.apply(i);
      DataType dataType = structField.dataType();
      partitionName.append(
          names[i]
              + PARTITION_VALUE_DELIMITER
              + SparkPartitionUtils.getPartitionValueAsString(ident, i, dataType));
      if (i < names.length - 1) {
        partitionName.append(PARTITION_NAME_DELIMITER);
      }
    }
    return partitionName.toString();
  }

  private @NotNull String getHivePartitionName(InternalRow ident, StructType partitionSchema) {
    StringBuilder partitionName = new StringBuilder();
    int numFields = ident.numFields();
    for (int i = 0; i < numFields; i++) {
      StructField structField = partitionSchema.apply(i);
      DataType dataType = structField.dataType();
      partitionName.append(
          structField.name()
              + PARTITION_VALUE_DELIMITER
              + SparkPartitionUtils.getPartitionValueAsString(ident, i, dataType));
      if (i < numFields - 1) {
        partitionName.append(PARTITION_NAME_DELIMITER);
      }
    }
    return partitionName.toString();
  }
}
