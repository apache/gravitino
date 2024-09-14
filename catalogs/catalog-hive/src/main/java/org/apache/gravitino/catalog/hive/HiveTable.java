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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.converter.HiveDataTypeConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/** Represents an Apache Hive Table entity in the Hive Metastore catalog. */
@ToString
public class HiveTable extends BaseTable {

  // A set of supported Hive table types.
  public static final Set<String> SUPPORT_TABLE_TYPES =
      Sets.newHashSet(TableType.MANAGED_TABLE.name(), TableType.EXTERNAL_TABLE.name());
  public static final String ICEBERG_TABLE_TYPE_VALUE = "ICEBERG";
  public static final String TABLE_TYPE_PROP = "table_type";
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
    AuditInfo auditInfo = HiveTableConverter.getAuditInfo(table);

    Distribution distribution = HiveTableConverter.getDistribution(table);

    SortOrder[] sortOrders = HiveTableConverter.getSortOrders(table);

    Column[] columns = HiveTableConverter.getColumns(table, HiveColumn.builder());

    Transform[] partitioning = HiveTableConverter.getPartitioning(table);

    return HiveTable.builder()
        .withName(table.getTableName())
        .withComment(table.getParameters().get(HiveTablePropertiesMetadata.COMMENT))
        .withProperties(buildTableProperties(table))
        .withColumns(columns)
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withAuditInfo(auditInfo)
        .withPartitioning(partitioning)
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

    Optional.ofNullable(table.getTableType())
        .ifPresent(t -> properties.put(HiveTablePropertiesMetadata.TABLE_TYPE, t));

    StorageDescriptor sd = table.getSd();
    properties.put(HiveTablePropertiesMetadata.LOCATION, sd.getLocation());
    properties.put(HiveTablePropertiesMetadata.INPUT_FORMAT, sd.getInputFormat());
    properties.put(HiveTablePropertiesMetadata.OUTPUT_FORMAT, sd.getOutputFormat());

    SerDeInfo serdeInfo = sd.getSerdeInfo();
    Optional.ofNullable(serdeInfo.getName())
        .ifPresent(name -> properties.put(HiveTablePropertiesMetadata.SERDE_NAME, name));
    Optional.ofNullable(serdeInfo.getSerializationLib())
        .ifPresent(lib -> properties.put(HiveTablePropertiesMetadata.SERDE_LIB, lib));
    Optional.ofNullable(serdeInfo.getParameters())
        .ifPresent(
            p ->
                p.forEach(
                    (k, v) ->
                        properties.put(HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX + k, v)));

    return properties;
  }

  /**
   * Converts this HiveTable to its corresponding Table in the Hive Metastore.
   *
   * @return The converted Table.
   */
  public Table toHiveTable(PropertiesMetadata tablePropertiesMetadata) {
    Table hiveTable = new Table();

    hiveTable.setTableName(name);
    hiveTable.setDbName(schemaName);
    hiveTable.setTableType(
        ((TableType)
                tablePropertiesMetadata.getOrDefault(
                    properties(), HiveTablePropertiesMetadata.TABLE_TYPE))
            .name());
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
    Optional.ofNullable(comment)
        .ifPresent(c -> parameters.put(HiveTablePropertiesMetadata.COMMENT, c));

    if (TableType.EXTERNAL_TABLE
        .name()
        .equalsIgnoreCase(properties().get(HiveTablePropertiesMetadata.TABLE_TYPE))) {
      parameters.put(HiveTablePropertiesMetadata.EXTERNAL, "TRUE");
    } else {
      parameters.put(HiveTablePropertiesMetadata.EXTERNAL, "FALSE");
    }

    parameters.remove(HiveTablePropertiesMetadata.LOCATION);
    parameters.remove(HiveTablePropertiesMetadata.TABLE_TYPE);
    parameters.remove(HiveTablePropertiesMetadata.INPUT_FORMAT);
    parameters.remove(HiveTablePropertiesMetadata.OUTPUT_FORMAT);
    parameters.remove(HiveTablePropertiesMetadata.SERDE_NAME);
    parameters.remove(HiveTablePropertiesMetadata.SERDE_LIB);
    parameters.remove(HiveTablePropertiesMetadata.FORMAT);
    parameters
        .keySet()
        .removeIf(k -> k.startsWith(HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX));
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
        HiveDataTypeConverter.CONVERTER
            .fromGravitino(partitionColumns.get(0).dataType())
            .getQualifiedName(),
        partitionColumns.get(0).comment());
  }

  private StorageDescriptor buildStorageDescriptor(
      PropertiesMetadata tablePropertiesMetadata, List<FieldSchema> partitionFields) {
    StorageDescriptor strgDesc = new StorageDescriptor();
    List<String> partitionKeys =
        partitionFields.stream().map(FieldSchema::getName).collect(Collectors.toList());
    strgDesc.setCols(
        Arrays.stream(columns)
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
    Optional.ofNullable(properties().get(HiveTablePropertiesMetadata.LOCATION))
        .ifPresent(
            l -> strgDesc.setLocation(properties().get(HiveTablePropertiesMetadata.LOCATION)));

    strgDesc.setSerdeInfo(buildSerDeInfo(tablePropertiesMetadata));
    StorageFormat storageFormat =
        (StorageFormat)
            tablePropertiesMetadata.getOrDefault(properties(), HiveTablePropertiesMetadata.FORMAT);
    strgDesc.setInputFormat(storageFormat.getInputFormat());
    strgDesc.setOutputFormat(storageFormat.getOutputFormat());
    // Individually specified INPUT_FORMAT and OUTPUT_FORMAT can override the inputFormat and
    // outputFormat of FORMAT
    Optional.ofNullable(properties().get(HiveTablePropertiesMetadata.INPUT_FORMAT))
        .ifPresent(strgDesc::setInputFormat);
    Optional.ofNullable(properties().get(HiveTablePropertiesMetadata.OUTPUT_FORMAT))
        .ifPresent(strgDesc::setOutputFormat);

    if (ArrayUtils.isNotEmpty(sortOrders)) {
      for (SortOrder sortOrder : sortOrders) {
        String columnName = ((NamedReference.FieldReference) sortOrder.expression()).fieldName()[0];
        strgDesc.addToSortCols(
            new Order(columnName, sortOrder.direction() == SortDirection.ASCENDING ? 1 : 0));
      }
    }

    if (!Distributions.NONE.equals(distribution)) {
      strgDesc.setBucketCols(
          Arrays.stream(distribution.expressions())
              .map(t -> ((NamedReference.FieldReference) t).fieldName()[0])
              .collect(Collectors.toList()));
      strgDesc.setNumBuckets(distribution.number());
    }

    return strgDesc;
  }

  private SerDeInfo buildSerDeInfo(PropertiesMetadata tablePropertiesMetadata) {
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setName(properties().getOrDefault(HiveTablePropertiesMetadata.SERDE_NAME, name()));

    StorageFormat storageFormat =
        (StorageFormat)
            tablePropertiesMetadata.getOrDefault(properties(), HiveTablePropertiesMetadata.FORMAT);
    serDeInfo.setSerializationLib(storageFormat.getSerde());
    // Individually specified SERDE_LIB can override the serdeLib of FORMAT
    Optional.ofNullable(properties().get(HiveTablePropertiesMetadata.SERDE_LIB))
        .ifPresent(serDeInfo::setSerializationLib);

    properties().entrySet().stream()
        .filter(e -> e.getKey().startsWith(HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX))
        .forEach(
            e ->
                serDeInfo.putToParameters(
                    e.getKey()
                        .substring(HiveTablePropertiesMetadata.SERDE_PARAMETER_PREFIX.length()),
                    e.getValue()));
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

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

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
        hiveTable.properties.put(HiveTablePropertiesMetadata.COMMENT, comment);
      }

      return hiveTable;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
