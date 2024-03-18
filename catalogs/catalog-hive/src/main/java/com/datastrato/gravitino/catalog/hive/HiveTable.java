/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.COMMENT;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.EXTERNAL;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.FORMAT;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.INPUT_FORMAT;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.LOCATION;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.OUTPUT_FORMAT;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.SERDE_LIB;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.SERDE_NAME;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TABLE_TYPE;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TableType.EXTERNAL_TABLE;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TableType.MANAGED_TABLE;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.identity;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TableType;
import com.datastrato.gravitino.catalog.hive.converter.FromHiveType;
import com.datastrato.gravitino.catalog.hive.converter.ToHiveType;
import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private String schemaName;
  private CachedClientPool clientPool;
  private StorageDescriptor sd;

  private HiveTable() {}

  /**
   * Creates a new HiveTable instance from a Table and a Builder.
   *
   * @param table The inner Table representing the HiveTable.
   * @return A new HiveTable instance Builder.
   */
  public static HiveTable.Builder fromHiveTable(Table table) {
    // Get audit info from Hive's Table object. Because Hive's table doesn't store last modifier
    // and last modified time, we only get creator and create time from Hive's table.
    AuditInfo.Builder auditInfoBuilder = AuditInfo.builder();
    Optional.ofNullable(table.getOwner()).ifPresent(auditInfoBuilder::withCreator);
    if (table.isSetCreateTime()) {
      auditInfoBuilder.withCreateTime(Instant.ofEpochSecond(table.getCreateTime()));
    }

    StorageDescriptor sd = table.getSd();
    Distribution distribution = Distributions.NONE;
    if (CollectionUtils.isNotEmpty(sd.getBucketCols())) {
      // Hive table use hash strategy as bucketing strategy
      distribution =
          Distributions.hash(
              sd.getNumBuckets(),
              sd.getBucketCols().stream().map(NamedReference::field).toArray(Expression[]::new));
    }

    SortOrder[] sortOrders = new SortOrder[0];
    if (CollectionUtils.isNotEmpty(sd.getSortCols())) {
      sortOrders =
          sd.getSortCols().stream()
              .map(
                  f ->
                      SortOrders.of(
                          NamedReference.field(f.getCol()),
                          f.getOrder() == 0 ? SortDirection.ASCENDING : SortDirection.DESCENDING))
              .toArray(SortOrder[]::new);
    }

    Column[] columns =
        Stream.concat(
                sd.getCols().stream()
                    .map(
                        f ->
                            new HiveColumn.Builder()
                                .withName(f.getName())
                                .withType(FromHiveType.convert(f.getType()))
                                .withComment(f.getComment())
                                .build()),
                table.getPartitionKeys().stream()
                    .map(
                        p ->
                            new HiveColumn.Builder()
                                .withName(p.getName())
                                .withType(FromHiveType.convert(p.getType()))
                                .withComment(p.getComment())
                                .build()))
            .toArray(Column[]::new);

    return new HiveTable.Builder()
        .withName(table.getTableName())
        .withComment(table.getParameters().get(COMMENT))
        .withProperties(buildTableProperties(table))
        .withColumns(columns)
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withAuditInfo(auditInfoBuilder.build())
        .withPartitioning(
            table.getPartitionKeys().stream()
                .map(p -> identity(p.getName()))
                .toArray(Transform[]::new))
        .withSchemaName(table.getDbName())
        .withStorageDescriptor(table.getSd());
  }

  public CachedClientPool clientPool() {
    return clientPool;
  }

  public void close() {
    if (clientPool != null) {
      // Note: Cannot close the client pool here because the client pool is shared by catalog
      clientPool = null;
    }
  }

  public String schemaName() {
    return schemaName;
  }

  public StorageDescriptor storageDescriptor() {
    return sd;
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
    List<FieldSchema> partitionFields = buildPartitionKeys();
    hiveTable.setSd(buildStorageDescriptor(tablePropertiesMetadata, partitionFields));
    hiveTable.setParameters(buildTableParameters());
    hiveTable.setPartitionKeys(partitionFields);

    // Set AuditInfo to Hive's Table object. Hive's Table doesn't support setting last modifier
    // and last modified time, so we only set creator and create time.
    hiveTable.setOwner(auditInfo.creator());
    hiveTable.setCreateTime(Math.toIntExact(auditInfo.createTime().getEpochSecond()));

    return hiveTable;
  }

  private Map<String, String> buildTableParameters() {
    Map<String, String> parameters = Maps.newHashMap(properties());
    Optional.ofNullable(comment).ifPresent(c -> parameters.put(COMMENT, c));
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

  public List<FieldSchema> buildPartitionKeys() {
    return Arrays.stream(partitioning)
        .map(p -> getPartitionKey(((Transforms.IdentityTransform) p).fieldName()))
        .collect(Collectors.toList());
  }

  private FieldSchema getPartitionKey(String[] fieldName) {
    List<Column> partitionColumns =
        Arrays.stream(columns)
            .filter(c -> c.name().equals(fieldName[0]))
            .collect(Collectors.toList());
    return new FieldSchema(
        partitionColumns.get(0).name(),
        ToHiveType.convert(partitionColumns.get(0).dataType()).getQualifiedName(),
        partitionColumns.get(0).comment());
  }

  private StorageDescriptor buildStorageDescriptor(
      HiveTablePropertiesMetadata tablePropertiesMetadata, List<FieldSchema> partitionFields) {
    StorageDescriptor sd = new StorageDescriptor();
    List<String> partitionKeys =
        partitionFields.stream().map(FieldSchema::getName).collect(Collectors.toList());
    sd.setCols(
        Arrays.stream(columns)
            .filter(c -> !partitionKeys.contains(c.name()))
            .map(
                c ->
                    new FieldSchema(
                        c.name(), ToHiveType.convert(c.dataType()).getQualifiedName(), c.comment()))
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
        String columnName = ((NamedReference.FieldReference) sortOrder.expression()).fieldName()[0];
        sd.addToSortCols(
            new Order(columnName, sortOrder.direction() == SortDirection.ASCENDING ? 0 : 1));
      }
    }

    if (!Distributions.NONE.equals(distribution)) {
      sd.setBucketCols(
          Arrays.stream(distribution.expressions())
              .map(t -> ((NamedReference.FieldReference) t).fieldName()[0])
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

  @Override
  protected TableOperations newOps() {
    return new HiveTableOperations(this);
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return (SupportsPartitions) ops();
  }

  /** A builder class for constructing HiveTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    private String schemaName;
    private CachedClientPool clientPool;
    private StorageDescriptor sd;

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
     * Sets the StorageDescriptor to be used for adding partition.
     *
     * @param sd The StorageDescriptor instance of the HiveTable.
     * @return This Builder instance.
     */
    public Builder withStorageDescriptor(StorageDescriptor sd) {
      this.sd = sd;
      return this;
    }

    /**
     * Sets the HiveClientPool to be used for operate partition.
     *
     * @param clientPool The HiveClientPool instance.
     * @return This Builder instance.
     */
    public Builder withClientPool(CachedClientPool clientPool) {
      this.clientPool = clientPool;
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
      hiveTable.partitioning = partitioning;
      hiveTable.schemaName = schemaName;
      hiveTable.clientPool = clientPool;
      hiveTable.sd = sd;
      hiveTable.proxyPlugin = proxyPlugin;

      // HMS put table comment in parameters
      if (comment != null) {
        hiveTable.properties.put(COMMENT, comment);
      }

      return hiveTable;
    }
  }
}
