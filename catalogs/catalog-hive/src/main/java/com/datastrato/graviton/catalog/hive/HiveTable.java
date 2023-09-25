/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
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

  // The key to store the Hive table comment in the parameters map.
  public static final String HMS_TABLE_COMMENT = "comment";

  private String location;

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
        .withComment(table.getParameters().get(HMS_TABLE_COMMENT))
        .withProperties(table.getParameters())
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
        .withLocation(table.getSd().getLocation())
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

  /**
   * Converts this HiveTable to its corresponding Table in the Hive Metastore.
   *
   * @return The converted Table.
   */
  public Table toHiveTable() {
    Table hiveTable = new Table();

    hiveTable.setTableName(name);
    hiveTable.setDbName(schemaName);
    hiveTable.setSd(buildStorageDescriptor());
    hiveTable.setParameters(properties);
    hiveTable.setPartitionKeys(buildPartitionKeys());

    // Set AuditInfo to Hive's Table object. Hive's Table doesn't support setting last modifier
    // and last modified time, so we only set creator and create time.
    hiveTable.setOwner(auditInfo.creator());
    hiveTable.setCreateTime(Math.toIntExact(auditInfo.createTime().getEpochSecond()));

    return hiveTable;
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

  private StorageDescriptor buildStorageDescriptor() {
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
    sd.setSerdeInfo(buildSerDeInfo());
    // `location` must not be null, otherwise it will result in an NPE when calling HMS `alterTable`
    // interface
    sd.setLocation(location);
    // TODO(minghuang): Remove hard coding below after supporting the specified Hive table
    //  properties
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");

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

  private SerDeInfo buildSerDeInfo() {
    // TODO(minghuang): Build SerDeInfo object based on user specifications.
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    return serDeInfo;
  }

  /** A builder class for constructing HiveTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    // currently, load from HMS only
    // TODO(minghuang): Support user specify`location` property
    private String location;

    private String schemaName;

    /**
     * Sets the location to be used for building the HiveTable.
     *
     * @param location The string location of the HiveTable.
     * @return This Builder instance.
     */
    public Builder withLocation(String location) {
      this.location = location;
      return this;
    }

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
      hiveTable.location = location;
      hiveTable.distribution = distribution;
      hiveTable.sortOrders = sortOrders;
      hiveTable.partitions = partitions;
      hiveTable.schemaName = schemaName;

      // HMS put table comment in parameters
      if (comment != null) {
        hiveTable.properties.put(HMS_TABLE_COMMENT, comment);
      }

      return hiveTable;
    }
  }
}
