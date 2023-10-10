/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.COMMENT;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.EXTERNAL;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.FORMAT;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.INPUT_FORMAT;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.LOCATION;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.OUTPUT_FORMAT;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.SERDE_LIB;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.SERDE_NAME;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX;
import static com.datastrato.graviton.catalog.hive.HiveTablePropertiesMetadata.TABLE_TYPE;
import static com.datastrato.graviton.rel.transforms.Transforms.identity;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

import com.datastrato.graviton.catalog.hive.converter.FromHiveType;
import com.datastrato.graviton.catalog.hive.converter.ToHiveType;
import com.datastrato.graviton.catalog.rel.BaseTable;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.datastrato.graviton.rel.transforms.Transforms.NamedReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/** Represents a Hive Table entity in the Hive Metastore catalog. */
@ToString
public class HiveTable extends BaseTable {

  // A set of supported Hive table types.
  public static final Set<String> SUPPORT_TABLE_TYPES =
      Sets.newHashSet(MANAGED_TABLE.name(), EXTERNAL_TABLE.name());
  private String schemaName;

  private HiveTable() {}

  /**
   * Creates a new HiveTable instance from a Table and a Builder.
   *
   * @param table The inner Table representing the HiveTable.
   * @return A new HiveTable instance.
   */
  public static HiveTable fromHiveTable(Table table) {
    // Get audit info from Hive's Table object. Because Hive's table doesn't store last modifier
    // and last modified time, we only get creator and create time from Hive's table.
    AuditInfo.Builder auditInfoBuilder = new AuditInfo.Builder();
    Optional.ofNullable(table.getOwner()).ifPresent(auditInfoBuilder::withCreator);
    if (table.isSetCreateTime()) {
      auditInfoBuilder.withCreateTime(Instant.ofEpochSecond(table.getCreateTime()));
    }

    Distribution distribution = Distribution.NONE;
    if (CollectionUtils.isNotEmpty(table.getSd().getBucketCols())) {
      distribution =
          Distribution.builder()
              .withTransforms(
                  table.getSd().getBucketCols().stream()
                      .map(f -> new NamedReference(new String[] {f}))
                      .toArray(Transform[]::new))
              .withNumber(table.getSd().getNumBuckets())
              // TODO(yuqi): support IDENTITY strategy for Hive catalog
              .withStrategy(Distribution.Strategy.HASH) // temporary work around
              .build();
    }

    SortOrder[] sortOrders = new SortOrder[0];
    if (CollectionUtils.isNotEmpty(table.getSd().getSortCols())) {
      sortOrders =
          table.getSd().getSortCols().stream()
              .map(
                  f ->
                      SortOrder.builder()
                          .withTransform(new NamedReference(new String[] {f.getCol()}))
                          .withDirection(f.getOrder() == 0 ? Direction.ASC : Direction.DESC)
                          .build())
              .toArray(SortOrder[]::new);
    }

    return new HiveTable.Builder()
        .withName(table.getTableName())
        .withComment(table.getParameters().get(COMMENT))
        .withProperties(buildTableProperties(table))
        .withColumns(
            table.getSd().getCols().stream()
                .map(
                    f ->
                        new HiveColumn.Builder()
                            .withName(f.getName())
                            .withType(FromHiveType.convert(f.getType()))
                            .withComment(f.getComment())
                            .build())
                .toArray(HiveColumn[]::new))
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withAuditInfo(auditInfoBuilder.build())
        .withPartitions(
            table.getPartitionKeys().stream()
                .map(p -> identity(new String[] {p.getName()}))
                .toArray(Transforms.NamedReference[]::new))
        .withSchemaName(table.getDbName())
        .build();
  }

  private static Map<String, String> buildTableProperties(Table table) {
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

  /**
   * Converts this HiveTable to its corresponding Table in the Hive Metastore.
   *
   * @return The converted Table.
   */
  public Table toHiveTable(HiveTablePropertiesMetadata tablePropertiesMetadata) {
    Table hiveTable = new Table();

    hiveTable.setTableName(name);
    hiveTable.setDbName(schemaName);
    hiveTable.setTableType(
        ((TableType) tablePropertiesMetadata.getOrDefault(properties(), TABLE_TYPE)).name());
    hiveTable.setSd(buildStorageDescriptor(tablePropertiesMetadata));
    hiveTable.setParameters(buildTableParameters());
    hiveTable.setPartitionKeys(buildPartitionKeys());

    // Set AuditInfo to Hive's Table object. Hive's Table doesn't support setting last modifier
    // and last modified time, so we only set creator and create time.
    hiveTable.setOwner(auditInfo.creator());
    hiveTable.setCreateTime(Math.toIntExact(auditInfo.createTime().getEpochSecond()));

    return hiveTable;
  }

  private Map<String, String> buildTableParameters() {
    Map<String, String> parameters = Maps.newHashMap(properties());
    parameters.put(COMMENT, comment);
    String ignore =
        EXTERNAL_TABLE.name().equalsIgnoreCase(properties().get(TABLE_TYPE))
            ? parameters.put(EXTERNAL, "TRUE")
            : parameters.put(EXTERNAL, "FALSE");

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

  private List<FieldSchema> buildPartitionKeys() {
    return Arrays.stream(partitions)
        .map(p -> getPartitionKey(((Transforms.NamedReference) p).value()))
        .collect(Collectors.toList());
  }

  private FieldSchema getPartitionKey(String[] fieldName) {
    Preconditions.checkArgument(
        fieldName.length == 1, "Hive partition does not support nested field");
    List<Column> partitionColumns =
        Arrays.stream(columns)
            .filter(c -> c.name().equals(fieldName[0]))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        partitionColumns.size() == 1, "Hive partition must match one column");
    return new FieldSchema(
        partitionColumns.get(0).name(),
        partitionColumns.get(0).dataType().accept(ToHiveType.INSTANCE).getQualifiedName(),
        partitionColumns.get(0).comment());
  }

  private StorageDescriptor buildStorageDescriptor(
      HiveTablePropertiesMetadata tablePropertiesMetadata) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(
        Arrays.stream(columns)
            .map(
                c ->
                    new FieldSchema(
                        c.name(),
                        c.dataType().accept(ToHiveType.INSTANCE).getQualifiedName(),
                        c.comment()))
            .collect(Collectors.toList()));

    // `location` must not be null, otherwise it will result in an NPE when calling HMS `alterTable`
    // interface
    Optional.ofNullable(properties().get(LOCATION))
        .ifPresent(l -> sd.setLocation(properties().get(LOCATION)));

    sd.setSerdeInfo(buildSerDeInfo(tablePropertiesMetadata));
    HiveTablePropertiesMetadata.StorageFormat storageFormat =
        (HiveTablePropertiesMetadata.StorageFormat)
            tablePropertiesMetadata.getOrDefault(properties(), FORMAT);
    sd.setInputFormat(storageFormat.getInputFormat());
    sd.setOutputFormat(storageFormat.getOutputFormat());
    // Individually specified INPUT_FORMAT and OUTPUT_FORMAT can override the inputFormat and
    // outputFormat of FORMAT
    Optional.ofNullable(properties().get(INPUT_FORMAT)).ifPresent(sd::setInputFormat);
    Optional.ofNullable(properties().get(OUTPUT_FORMAT)).ifPresent(sd::setOutputFormat);

    if (ArrayUtils.isNotEmpty(sortOrders)) {
      for (SortOrder sortOrder : sortOrders) {
        String columnName = ((NamedReference) sortOrder.getTransform()).value()[0];
        sd.addToSortCols(new Order(columnName, sortOrder.getDirection() == Direction.ASC ? 0 : 1));
      }
    }

    if (!Distribution.NONE.equals(distribution)) {
      sd.setBucketCols(
          Arrays.stream(distribution.transforms())
              .map(t -> ((NamedReference) t).value()[0])
              .collect(Collectors.toList()));
      sd.setNumBuckets(distribution.number());
    }

    return sd;
  }

  private SerDeInfo buildSerDeInfo(HiveTablePropertiesMetadata tablePropertiesMetadata) {
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setName(properties().getOrDefault(SERDE_NAME, name()));

    HiveTablePropertiesMetadata.StorageFormat storageFormat =
        (HiveTablePropertiesMetadata.StorageFormat)
            tablePropertiesMetadata.getOrDefault(properties(), FORMAT);
    serDeInfo.setSerializationLib(storageFormat.getSerde());
    // Individually specified SERDE_LIB can override the serdeLib of FORMAT
    Optional.ofNullable(properties().get(SERDE_LIB)).ifPresent(serDeInfo::setSerializationLib);

    properties().entrySet().stream()
        .filter(e -> e.getKey().startsWith(SERDE_PARAMETER_PREFIX))
        .forEach(
            e ->
                serDeInfo.putToParameters(
                    e.getKey().substring(SERDE_PARAMETER_PREFIX.length()), e.getValue()));
    return serDeInfo;
  }

  /** A builder class for constructing HiveTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    private String schemaName;

    /**
     * Sets the Hive schema (database) name to be used for building the HiveTable.
     *
     * @param schemaName The string schema name of the HiveTable.
     * @return This Builder instance.
     */
    public Builder withSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    /**
     * Internal method to build a HiveTable instance using the provided values.
     *
     * @return A new HiveTable instance with the configured values.
     */
    @Override
    protected HiveTable internalBuild() {
      HiveTable hiveTable = new HiveTable();
      hiveTable.name = name;
      hiveTable.comment = comment;
      hiveTable.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      hiveTable.auditInfo = auditInfo;
      hiveTable.columns = columns;
      hiveTable.distribution = distribution;
      hiveTable.sortOrders = sortOrders;
      hiveTable.partitions = partitions;
      hiveTable.schemaName = schemaName;

      // HMS put table comment in parameters
      if (comment != null) {
        hiveTable.properties.put(COMMENT, comment);
      }

      return hiveTable;
    }
  }
}
