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
import org.apache.gravitino.hive.HiveColumn;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Type;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

public class HiveTableConverter {

  public static HiveTable fromHiveTable(org.apache.hadoop.hive.metastore.api.Table table) {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    AuditInfo auditInfo = HiveTableConverter.getAuditInfo(table);

    Distribution distribution = HiveTableConverter.getDistribution(table);

    SortOrder[] sortOrders = HiveTableConverter.getSortOrders(table);

    Column[] columns = HiveTableConverter.getColumns(table);

    Transform[] partitioning = HiveTableConverter.getPartitioning(table);

    String catalogName = null;
    try {
      java.lang.reflect.Method getCatNameMethod = table.getClass().getMethod("getCatName");
      catalogName = (String) getCatNameMethod.invoke(table);
    } catch (Exception e) {
      // Hive2 doesn't have getCatName method, catalogName will be null
    }

    HiveTable hiveTable =
        HiveTable.builder()
            .withName(table.getTableName())
            .withComment(table.getParameters().get(COMMENT))
            .withProperties(buildTableProperties(table))
            .withColumns(columns)
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withAuditInfo(auditInfo)
            .withPartitioning(partitioning)
            .withCatalogName(catalogName)
            .withDatabaseName(table.getDbName())
            .build();
    return hiveTable;
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

  public static org.apache.hadoop.hive.metastore.api.Table toHiveTable(HiveTable hiveTable) {
    Preconditions.checkArgument(hiveTable != null, "HiveTable cannot be null");
    String dbName = hiveTable.databaseName();
    Preconditions.checkArgument(dbName != null, "Database name cannot be null");

    org.apache.hadoop.hive.metastore.api.Table table =
        new org.apache.hadoop.hive.metastore.api.Table();

    table.setTableName(hiveTable.name());
    table.setDbName(dbName);
    String tableType =
        hiveTable.properties().getOrDefault(TABLE_TYPE, String.valueOf(TableType.MANAGED_TABLE));
    table.setTableType(tableType.toUpperCase());

    List<FieldSchema> partitionFields =
        hiveTable.partitionFieldNames().stream()
            .map(fieldName -> buildPartitionKeyField(fieldName, hiveTable))
            .collect(Collectors.toList());
    table.setSd(buildStorageDescriptor(hiveTable, partitionFields));
    table.setParameters(buildTableParameters(hiveTable));
    table.setPartitionKeys(partitionFields);

    // Set AuditInfo to Hive's Table object. Hive's Table doesn't support setting last modifier
    // and last modified time, so we only set creator and create time.
    table.setOwner(hiveTable.auditInfo().creator());
    table.setCreateTime(Math.toIntExact(hiveTable.auditInfo().createTime().getEpochSecond()));

    return table;
  }

  private static FieldSchema buildPartitionKeyField(String fieldName, HiveTable table) {
    Column partitionColumn =
        Arrays.stream(table.columns())
            .filter(c -> c.name().equals(fieldName))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Partition column %s does not exist", fieldName)));
    return new FieldSchema(
        partitionColumn.name(),
        HiveDataTypeConverter.CONVERTER
            .fromGravitino(partitionColumn.dataType())
            .getQualifiedName(),
        partitionColumn.comment());
  }

  private static StorageDescriptor buildStorageDescriptor(
      HiveTable table, List<FieldSchema> partitionFields) {
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
    Optional.ofNullable(table.properties().get(LOCATION)).ifPresent(strgDesc::setLocation);

    strgDesc.setSerdeInfo(buildSerDeInfo(table));
    StorageFormat storageFormat =
        StorageFormat.valueOf(
            table
                .properties()
                .getOrDefault(FORMAT, String.valueOf(StorageFormat.TEXTFILE))
                .toUpperCase());
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

  private static SerDeInfo buildSerDeInfo(HiveTable table) {
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setName(table.properties().getOrDefault(SERDE_NAME, table.name()));

    StorageFormat storageFormat =
        StorageFormat.valueOf(
            table
                .properties()
                .getOrDefault(FORMAT, String.valueOf(StorageFormat.TEXTFILE))
                .toUpperCase());
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

  private static Map<String, String> buildTableParameters(HiveTable table) {
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

  public static Column[] getColumns(org.apache.hadoop.hive.metastore.api.Table table) {
    StorageDescriptor sd = table.getSd();
    // Collect column names from sd.getCols() to check for duplicates
    Set<String> columnNames =
        sd.getCols().stream().map(FieldSchema::getName).collect(Collectors.toSet());

    return Stream.concat(
            sd.getCols().stream()
                .map(
                    f ->
                        buildColumn(
                            f.getName(),
                            HiveDataTypeConverter.CONVERTER.toGravitino(f.getType()),
                            f.getComment())),
            table.getPartitionKeys().stream()
                // Filter out partition keys that already exist in sd.getCols()
                .filter(p -> !columnNames.contains(p.getName()))
                .map(
                    p ->
                        buildColumn(
                            p.getName(),
                            HiveDataTypeConverter.CONVERTER.toGravitino(p.getType()),
                            p.getComment())))
        .toArray(Column[]::new);
  }

  private static Column buildColumn(String name, Type type, String comment) {
    HiveColumn.Builder builder =
        HiveColumn.builder().withName(name).withType(type).withNullable(true);
    if (comment != null) {
      builder.withComment(comment);
    }
    return builder.build();
  }

  public static HivePartition fromHivePartition(
      HiveTable table, org.apache.hadoop.hive.metastore.api.Partition partition) {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    Preconditions.checkArgument(partition != null, "Partition cannot be null");
    List<String> partitionColumns = table.partitionFieldNames();
    String partitionName = FileUtils.makePartName(partitionColumns, partition.getValues());
    // todo: support partition properties metadata to get more necessary information
    return HivePartition.identity(partitionName, partition.getParameters());
  }

  public static org.apache.hadoop.hive.metastore.api.Partition toHivePartition(
      String dbName, HiveTable table, HivePartition partition) {
    Preconditions.checkArgument(dbName != null, "Database name cannot be null");
    Preconditions.checkArgument(table != null, "Table cannot be null");
    Preconditions.checkArgument(partition != null, "Partition cannot be null");
    org.apache.hadoop.hive.metastore.api.Partition hivePartition =
        new org.apache.hadoop.hive.metastore.api.Partition();
    hivePartition.setDbName(dbName);
    hivePartition.setTableName(table.name());

    List<FieldSchema> partitionFields =
        table.partitionFieldNames().stream()
            .map(fieldName -> buildPartitionKeyField(fieldName, table))
            .collect(Collectors.toList());
    // todo: support custom serde and location if necessary
    StorageDescriptor sd = buildStorageDescriptor(table, partitionFields);
    // The location will be automatically generated by Hive Metastore
    sd.setLocation(null);

    hivePartition.setSd(sd);
    hivePartition.setParameters(partition.properties());

    List<String> values = HivePartition.extractPartitionValues(partition.name());
    hivePartition.setValues(values);
    return hivePartition;
  }
}
