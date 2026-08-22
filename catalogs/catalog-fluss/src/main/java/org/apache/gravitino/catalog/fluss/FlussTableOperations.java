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

package org.apache.gravitino.catalog.fluss;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;

final class FlussTableOperations implements TableOperations, SupportsPartitions {

  private final FlussAdminOps ops;
  private final TablePath tablePath;
  private final List<String> partitionKeys;

  FlussTableOperations(FlussAdminOps ops, TablePath tablePath, List<String> partitionKeys) {
    this.ops = ops;
    this.tablePath = tablePath;
    this.partitionKeys =
        partitionKeys == null ? Collections.emptyList() : List.copyOf(partitionKeys);
  }

  @Override
  public String[] listPartitionNames() {
    return Stream.of(listPartitions()).map(Partition::name).toArray(String[]::new);
  }

  @Override
  public Partition[] listPartitions() {
    List<PartitionInfo> partitions =
        ops.doAsAdmin(
            admin -> admin.listPartitionInfos(tablePath),
            FlussExceptionConverter.forPartition(
                tablePath, "Failed to list Fluss partitions for " + tablePath));
    return partitions.stream()
        .map(FlussTableOperations::toGravitinoPartition)
        .toArray(Partition[]::new);
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    PartitionSpec spec = toFlussPartitionSpec(partitionName);
    Optional<PartitionInfo> partitionInfo =
        listFlussPartitionInfos(spec, partitionName).stream()
            .filter(info -> partitionName.equals(toPartitionName(info.getPartitionSpec())))
            .findFirst();
    return partitionInfo
        .map(FlussTableOperations::toGravitinoPartition)
        .orElseThrow(
            () ->
                new NoSuchPartitionException(
                    FlussExceptionConverter.PARTITION_DOES_NOT_EXIST_MESSAGE,
                    partitionName,
                    tablePath));
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    PartitionSpec spec = toFlussPartitionSpec(partition);
    ops.doAsAdmin(
        admin -> admin.createPartition(tablePath, spec, false),
        FlussExceptionConverter.forPartition(
            tablePath, partition.name(), "Failed to add Fluss partition to " + tablePath));
    return toGravitinoPartition(toPartitionName(spec), spec);
  }

  @Override
  public boolean dropPartition(String partitionName) {
    PartitionSpec spec = toFlussPartitionSpec(partitionName);
    try {
      ops.doAsAdmin(
          admin -> admin.dropPartition(tablePath, spec, false),
          FlussExceptionConverter.forPartition(
              tablePath,
              partitionName,
              "Failed to drop Fluss partition " + partitionName + " from table " + tablePath));
    } catch (NoSuchTableException | NoSuchPartitionException e) {
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {}

  private List<PartitionInfo> listFlussPartitionInfos(PartitionSpec spec, String partitionName) {
    return ops.doAsAdmin(
        admin -> admin.listPartitionInfos(tablePath, spec),
        FlussExceptionConverter.forPartition(
            tablePath, partitionName, "Failed to get Fluss partition " + partitionName));
  }

  private PartitionSpec toFlussPartitionSpec(Partition partition) {
    if (!(partition instanceof IdentityPartition identityPartition)) {
      throw new IllegalArgumentException("Fluss only supports identity partitions");
    }

    String[][] fieldNames = identityPartition.fieldNames();
    Literal<?>[] values = identityPartition.values();
    if (fieldNames.length != values.length) {
      throw new IllegalArgumentException("Partition fields and values must have the same length");
    }
    if (fieldNames.length != partitionKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Partition fields count (%s) must match Fluss partition keys count (%s)",
              fieldNames.length, partitionKeys.size()));
    }

    Map<String, String> specMap = new LinkedHashMap<>();
    for (int i = 0; i < fieldNames.length; i++) {
      if (fieldNames[i].length != 1) {
        throw new IllegalArgumentException("Fluss only supports top-level identity partitions");
      }
      if (!fieldNames[i][0].equals(partitionKeys.get(i))) {
        throw new IllegalArgumentException(
            String.format(
                "Partition field %s must match Fluss partition key %s",
                fieldNames[i][0], partitionKeys.get(i)));
      }
      specMap.put(fieldNames[i][0], String.valueOf(values[i].value()));
    }
    return new PartitionSpec(specMap);
  }

  private PartitionSpec toFlussPartitionSpec(String partitionName) {
    Map<String, String> specMap = new LinkedHashMap<>();
    if (partitionKeys.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(FlussExceptionConverter.TABLE_IS_NOT_PARTITIONED_MESSAGE, tablePath));
    }

    String[] fields = partitionName.split("/", -1);
    if (fields.length != partitionKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Partition name '%s' has %s segment(s) but Fluss table has %s partition key(s)",
              partitionName, fields.length, partitionKeys.size()));
    }

    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      int index = field.indexOf('=');
      if (index <= 0) {
        throw new IllegalArgumentException("Invalid Fluss partition name: " + partitionName);
      }
      String key = field.substring(0, index);
      if (!key.equals(partitionKeys.get(i))) {
        throw new IllegalArgumentException(
            String.format(
                "Partition key %s must match Fluss partition key %s", key, partitionKeys.get(i)));
      }
      specMap.put(key, field.substring(index + 1));
    }
    return new PartitionSpec(specMap);
  }

  private static IdentityPartition toGravitinoPartition(PartitionInfo partitionInfo) {
    PartitionSpec spec = partitionInfo.getPartitionSpec();
    return toGravitinoPartition(toPartitionName(spec), spec);
  }

  private static IdentityPartition toGravitinoPartition(String name, PartitionSpec spec) {
    Map<String, String> specMap = spec.getSpecMap();
    String[][] fieldNames = new String[specMap.size()][];
    Literal<?>[] values = new Literal<?>[specMap.size()];

    int i = 0;
    for (Map.Entry<String, String> entry : specMap.entrySet()) {
      fieldNames[i] = new String[] {entry.getKey()};
      values[i] = Literals.stringLiteral(entry.getValue());
      i++;
    }

    return Partitions.identity(name, fieldNames, values, null);
  }

  private static String toPartitionName(PartitionSpec spec) {
    return spec.getSpecMap().entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining("/"));
  }
}
