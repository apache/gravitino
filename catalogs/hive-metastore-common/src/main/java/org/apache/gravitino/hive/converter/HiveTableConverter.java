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
package org.apache.gravitino.hive.converter;

import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;
import static org.apache.gravitino.catalog.hive.HiveConstants.EXTERNAL;
import static org.apache.gravitino.catalog.hive.HiveConstants.FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.INPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.LOCATION;
import static org.apache.gravitino.catalog.hive.HiveConstants.OUTPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_LIB;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_NAME;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_PARAMETER_PREFIX;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.catalog.hive.StorageFormat;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

public class HiveTableConverter {

  private static final String PARTITION_NAME_DELIMITER = "/";
  private static final String PARTITION_VALUE_DELIMITER = "=";

  public static Table fromHiveTable(org.apache.hadoop.hive.metastore.api.Table table) {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    AuditInfo auditInfo = HiveTableConverter.getAuditInfo(table);

    Distribution distribution = HiveTableConverter.getDistribution(table);

    SortOrder[] sortOrders = HiveTableConverter.getSortOrders(table);

    Column[] columns = HiveTableConverter.getColumns(table, ColumnDTO.builder());

    Transform[] partitioning = HiveTableConverter.getPartitioning(table);

    return TableDTO.builder()
        .withName(table.getTableName())
        .withComment(table.getParameters().get(COMMENT))
        .withProperties(buildTableProperties(table))
        .withColumns(Arrays.stream(columns).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new))
        .withDistribution(DTOConverters.toDTO(distribution))
        .withSortOrders(DTOConverters.toDTOs(sortOrders))
        .withAudit(DTOConverters.toDTO(auditInfo))
        .withPartitioning(DTOConverters.toDTOs(partitioning))
        .build();
  }

  private static Map<String, String> buildTableProperties(
      org.apache.hadoop.hive.metastore.api.Table table) {
    Map<String, String> properties = Maps.newHashMap(table.getParameters());

    Optional.ofNullable(table.getTableType()).ifPresent(t -> properties.put(TABLE_TYPE, t));

    StorageDescriptor sd = table.getSd();
    properties.put(LOCATION, sd.getLocation());
    properties.put(INPUT_FORMAT, sd.getInputFormat());
    properties.put(OUTPUT_FORMAT, sd.getOutputFormat());

    SerDeInfo serdeInfo = sd.getSerdeInfo();
    Optional.ofNullable(serdeInfo.getName()).ifPresent(name -> properties.put(SERDE_NAME, name));
    Optional.ofNullable(serdeInfo.getSerializationLib())
        .ifPresent(lib -> properties.put(SERDE_LIB, lib));
    Optional.ofNullable(serdeInfo.getParameters())
        .ifPresent(p -> p.forEach((k, v) -> properties.put(SERDE_PARAMETER_PREFIX + k, v)));

    return properties;
  }

  public static org.apache.hadoop.hive.metastore.api.Table toHiveTable(String dbName, Table table) {
    Preconditions.checkArgument(dbName != null, "Database name cannot be null");
    Preconditions.checkArgument(table != null, "Table cannot be null");
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        new org.apache.hadoop.hive.metastore.api.Table();

    hiveTable.setTableName(table.name());
    hiveTable.setDbName(dbName);
    String tableType =
        table.properties().getOrDefault(TABLE_TYPE, String.valueOf(TableType.MANAGED_TABLE));
    hiveTable.setTableType(tableType);

    List<FieldSchema> partitionFields = buildPartitionKeys(table);
    hiveTable.setSd(buildStorageDescriptor(table, partitionFields));
    hiveTable.setParameters(buildTableParameters(table));
    hiveTable.setPartitionKeys(partitionFields);

    // Set AuditInfo to Hive's Table object. Hive's Table doesn't support setting last modifier
    // and last modified time, so we only set creator and create time.
    hiveTable.setOwner(table.auditInfo().creator());
    hiveTable.setCreateTime(Math.toIntExact(table.auditInfo().createTime().getEpochSecond()));

    return hiveTable;
  }

  public static List<FieldSchema> buildPartitionKeys(Table table) {
    return Arrays.stream(table.partitioning())
        .map(HiveTableConverter::getPartitionFieldNames)
        .map(fieldName -> getPartitionKey(fieldName, table))
        .collect(Collectors.toList());
  }

  private static String[] getPartitionFieldNames(Transform partitioning) {
    if (partitioning instanceof Transforms.IdentityTransform) {
      return ((Transforms.IdentityTransform) partitioning).fieldName();
    }

    if (partitioning instanceof Partitioning partitioningDTO) {
      if (partitioningDTO instanceof Partitioning.SingleFieldPartitioning singleFieldPartitioning) {
        return singleFieldPartitioning.fieldName();
      }
    }

    if (partitioning.arguments().length > 0
        && partitioning.arguments()[0] instanceof NamedReference.FieldReference fieldReference) {
      return fieldReference.fieldName();
    }

    throw new IllegalArgumentException(
        String.format("Unsupported partition transform type: %s", partitioning.getClass()));
  }

  private static FieldSchema getPartitionKey(String[] fieldName, Table table) {
    List<Column> partitionColumns =
        Arrays.stream(table.columns())
            .filter(c -> c.name().equals(fieldName[0]))
            .collect(Collectors.toList());
    return new FieldSchema(
        partitionColumns.get(0).name(),
        HiveDataTypeConverter.CONVERTER
            .fromGravitino(partitionColumns.get(0).dataType())
            .getQualifiedName(),
        partitionColumns.get(0).comment());
  }

  private static StorageDescriptor buildStorageDescriptor(
      Table table, List<FieldSchema> partitionFields) {
    StorageDescriptor strgDesc = new StorageDescriptor();
    List<String> partitionKeys =
        partitionFields.stream().map(FieldSchema::getName).collect(Collectors.toList());
    strgDesc.setCols(
        Arrays.stream(table.columns())
            .filter(c -> !partitionKeys.contains(c.name()))
            .map(
                c ->
                    new FieldSchema(
                        c.name(),
                        HiveDataTypeConverter.CONVERTER
                            .fromGravitino(c.dataType())
                            .getQualifiedName(),
                        c.comment()))
            .collect(Collectors.toList()));

    // `location` must not be null, otherwise it will result in an NPE when calling HMS `alterTable`
    // interface
    Optional.ofNullable(table.properties().get(LOCATION))
        .ifPresent(l -> strgDesc.setLocation(table.properties().get(LOCATION)));

    strgDesc.setSerdeInfo(buildSerDeInfo(table));
    StorageFormat storageFormat =
        StorageFormat.valueOf(
            table.properties().getOrDefault(FORMAT, String.valueOf(StorageFormat.TEXTFILE)));
    strgDesc.setInputFormat(storageFormat.getInputFormat());
    strgDesc.setOutputFormat(storageFormat.getOutputFormat());
    // Individually specified INPUT_FORMAT and OUTPUT_FORMAT can override the inputFormat and
    // outputFormat of FORMAT
    Optional.ofNullable(table.properties().get(INPUT_FORMAT)).ifPresent(strgDesc::setInputFormat);
    Optional.ofNullable(table.properties().get(OUTPUT_FORMAT)).ifPresent(strgDesc::setOutputFormat);

    if (table.sortOrder() != null && table.sortOrder().length > 0) {
      for (SortOrder sortOrder : table.sortOrder()) {
        String columnName = ((NamedReference.FieldReference) sortOrder.expression()).fieldName()[0];
        strgDesc.addToSortCols(
            new Order(columnName, sortOrder.direction() == SortDirection.ASCENDING ? 1 : 0));
      }
    }

    if (table.distribution() != null && !Distributions.NONE.equals(table.distribution())) {
      strgDesc.setBucketCols(
          Arrays.stream(table.distribution().expressions())
              .map(t -> ((NamedReference.FieldReference) t).fieldName()[0])
              .collect(Collectors.toList()));
      strgDesc.setNumBuckets(table.distribution().number());
    }

    return strgDesc;
  }

  private static SerDeInfo buildSerDeInfo(Table table) {
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setName(table.properties().getOrDefault(SERDE_NAME, table.name()));

    StorageFormat storageFormat =
        StorageFormat.valueOf(
            table.properties().getOrDefault(FORMAT, String.valueOf(StorageFormat.TEXTFILE)));
    serDeInfo.setSerializationLib(storageFormat.getSerde());
    // Individually specified SERDE_LIB can override the serdeLib of FORMAT
    Optional.ofNullable(table.properties().get(SERDE_LIB))
        .ifPresent(serDeInfo::setSerializationLib);

    table.properties().entrySet().stream()
        .filter(e -> e.getKey().startsWith(SERDE_PARAMETER_PREFIX))
        .forEach(
            e ->
                serDeInfo.putToParameters(
                    e.getKey().substring(SERDE_PARAMETER_PREFIX.length()), e.getValue()));
    return serDeInfo;
  }

  private static Map<String, String> buildTableParameters(Table table) {
    Map<String, String> parameters = Maps.newHashMap(table.properties());
    Optional.ofNullable(table.comment()).ifPresent(c -> parameters.put(COMMENT, c));

    if (TableType.EXTERNAL_TABLE.name().equalsIgnoreCase(table.properties().get(TABLE_TYPE))) {
      parameters.put(EXTERNAL, "TRUE");
    } else {
      parameters.put(EXTERNAL, "FALSE");
    }

    parameters.remove(LOCATION);
    parameters.remove(TABLE_TYPE);
    parameters.remove(INPUT_FORMAT);
    parameters.remove(OUTPUT_FORMAT);
    parameters.remove(SERDE_NAME);
    parameters.remove(SERDE_LIB);
    parameters.remove(FORMAT);
    parameters.keySet().removeIf(k -> k.startsWith(SERDE_PARAMETER_PREFIX));
    return parameters;
  }

  public static AuditInfo getAuditInfo(org.apache.hadoop.hive.metastore.api.Table table) {
    // Get audit info from Hive's Table object. Because Hive's table doesn't store last modifier
    // and last modified time, we only get creator and create time from Hive's table.
    AuditInfo.Builder auditInfoBuilder = AuditInfo.builder();
    Optional.ofNullable(table.getOwner()).ifPresent(auditInfoBuilder::withCreator);
    if (table.isSetCreateTime()) {
      auditInfoBuilder.withCreateTime(Instant.ofEpochSecond(table.getCreateTime()));
    }
    return auditInfoBuilder.build();
  }

  public static Distribution getDistribution(org.apache.hadoop.hive.metastore.api.Table table) {
    StorageDescriptor sd = table.getSd();
    Distribution distribution = Distributions.NONE;
    if (sd.getBucketCols() != null && !sd.getBucketCols().isEmpty()) {
      // Hive table use hash strategy as bucketing strategy
      distribution =
          Distributions.hash(
              sd.getNumBuckets(),
              sd.getBucketCols().stream().map(NamedReference::field).toArray(Expression[]::new));
    }
    return distribution;
  }

  public static SortOrder[] getSortOrders(org.apache.hadoop.hive.metastore.api.Table table) {
    SortOrder[] sortOrders = SortOrders.NONE;
    StorageDescriptor sd = table.getSd();
    if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
      sortOrders =
          sd.getSortCols().stream()
              .map(
                  f ->
                      SortOrders.of(
                          NamedReference.field(f.getCol()),
                          f.getOrder() == 1 ? SortDirection.ASCENDING : SortDirection.DESCENDING))
              .toArray(SortOrder[]::new);
    }
    return sortOrders;
  }

  public static Transform[] getPartitioning(org.apache.hadoop.hive.metastore.api.Table table) {
    return table.getPartitionKeys().stream()
        .map(p -> identity(p.getName()))
        .toArray(Transform[]::new);
  }

  public static ColumnDTO[] getColumns(
      org.apache.hadoop.hive.metastore.api.Table table, ColumnDTO.Builder columnBuilder) {
    StorageDescriptor sd = table.getSd();
    // Collect column names from sd.getCols() to check for duplicates
    Set<String> columnNames =
        sd.getCols().stream().map(FieldSchema::getName).collect(Collectors.toSet());

    return Stream.concat(
            sd.getCols().stream()
                .map(
                    f ->
                        columnBuilder
                            .withName(f.getName())
                            .withDataType(HiveDataTypeConverter.CONVERTER.toGravitino(f.getType()))
                            .withComment(f.getComment())
                            .build()),
            table.getPartitionKeys().stream()
                // Filter out partition keys that already exist in sd.getCols()
                .filter(p -> !columnNames.contains(p.getName()))
                .map(
                    p ->
                        columnBuilder
                            .withName(p.getName())
                            .withDataType(HiveDataTypeConverter.CONVERTER.toGravitino(p.getType()))
                            .withComment(p.getComment())
                            .build()))
        .toArray(ColumnDTO[]::new);
  }

  public static Partition fromHivePartition(
      Table table, org.apache.hadoop.hive.metastore.api.Partition partition) {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    Preconditions.checkArgument(partition != null, "Partition cannot be null");
    List<String> partCols =
        buildPartitionKeys(table).stream().map(FieldSchema::getName).collect(Collectors.toList());
    String partitionName = FileUtils.makePartName(partCols, partition.getValues());
    String[][] fieldNames = getFieldNames(partitionName);
    Literal[] values =
        partition.getValues().stream().map(Literals::stringLiteral).toArray(Literal[]::new);
    // todo: support partition properties metadata to get more necessary information
    return Partitions.identity(partitionName, fieldNames, values, partition.getParameters());
  }

  private static String[][] getFieldNames(String partitionName) {
    // Hive partition name is in the format of "field1=value1/field2=value2/..."
    String[] fields = partitionName.split(PARTITION_NAME_DELIMITER);
    return Arrays.stream(fields)
        .map(field -> new String[] {field.split(PARTITION_VALUE_DELIMITER)[0]})
        .toArray(String[][]::new);
  }

  public static java.util.List<String> parsePartitionValues(String partitionName) {
    if (partitionName == null || partitionName.isEmpty()) {
      return java.util.List.of();
    }
    return Arrays.stream(partitionName.split(PARTITION_NAME_DELIMITER))
        .map(
            field -> {
              String[] kv = field.split(PARTITION_VALUE_DELIMITER, 2);
              return kv.length > 1 ? kv[1] : "";
            })
        .collect(Collectors.toList());
  }

  public static org.apache.hadoop.hive.metastore.api.Partition toHivePartition(
      String dbName, Table table, Partition partition) {
    Preconditions.checkArgument(dbName != null, "Database name cannot be null");
    Preconditions.checkArgument(table != null, "Table cannot be null");
    Preconditions.checkArgument(partition != null, "Partition cannot be null");
    org.apache.hadoop.hive.metastore.api.Partition hivePartition =
        new org.apache.hadoop.hive.metastore.api.Partition();
    hivePartition.setDbName(dbName);
    hivePartition.setTableName(table.name());

    // todo: support custom serde and location if necessary
    StorageDescriptor sd = buildStorageDescriptor(table, buildPartitionKeys(table));
    hivePartition.setSd(sd);
    hivePartition.setParameters(partition.properties());

    List<String> values = parsePartitionValues(partition.name());
    hivePartition.setValues(values);
    return hivePartition;
  }
}
