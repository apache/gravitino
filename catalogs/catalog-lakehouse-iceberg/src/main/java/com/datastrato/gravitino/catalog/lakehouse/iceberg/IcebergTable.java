/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.DISTRIBUTION_MODE;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.FORMAT;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.FORMAT_VERSION;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.LOCATION;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.PROVIDER;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.SORT_ORDER;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.DescribeIcebergSortOrderVisitor;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergSortOrder;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ToIcebergPartitionSpec;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ToIcebergSortOrder;
import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.transforms.SortOrderVisitor;

/** Represents an Iceberg Table entity in the Iceberg table. */
@ToString
@Getter
public class IcebergTable extends BaseTable {

  /**
   * A reserved property to specify the location of the table. The files of the table should be
   * under this location.
   */
  public static final String PROP_LOCATION = "location";

  /** A reserved property to specify the provider of the table. */
  public static final String PROP_PROVIDER = "provider";

  /** The default provider of the table. */
  public static final String DEFAULT_ICEBERG_PROVIDER = "iceberg";

  /** The supported parquet file format for Iceberg tables. */
  public static final String ICEBERG_PARQUET_FILE_FORMAT = "parquet";
  /** The supported orc file format for Iceberg tables. */
  public static final String ICEBERG_ORC_FILE_FORMAT = "orc";
  /** The supported avro file format for Iceberg tables. */
  public static final String ICEBERG_AVRO_FILE_FORMAT = "avro";

  public static final String ICEBERG_COMMENT_FIELD_NAME = "comment";

  private String location;

  private IcebergTable() {}

  public static Map<String, String> rebuildCreateProperties(Map<String, String> createProperties) {
    String provider = createProperties.get(PROP_PROVIDER);
    if (ICEBERG_PARQUET_FILE_FORMAT.equalsIgnoreCase(provider)) {
      createProperties.put(DEFAULT_FILE_FORMAT, ICEBERG_PARQUET_FILE_FORMAT);
    } else if (ICEBERG_AVRO_FILE_FORMAT.equalsIgnoreCase(provider)) {
      createProperties.put(DEFAULT_FILE_FORMAT, ICEBERG_AVRO_FILE_FORMAT);
    } else if (ICEBERG_ORC_FILE_FORMAT.equalsIgnoreCase(provider)) {
      createProperties.put(DEFAULT_FILE_FORMAT, ICEBERG_ORC_FILE_FORMAT);
    } else if (provider != null && !DEFAULT_ICEBERG_PROVIDER.equalsIgnoreCase(provider)) {
      throw new IllegalArgumentException("Unsupported format in USING: " + provider);
    }
    return createProperties;
  }

  public CreateTableRequest toCreateTableRequest() {
    Schema schema = ConvertUtil.toIcebergSchema(this);
    properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
    properties.put(DISTRIBUTION_MODE, transformDistribution(distribution));
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .withName(name)
            .withLocation(location)
            .withSchema(schema)
            .setProperties(rebuildCreateProperties(properties))
            .withPartitionSpec(ToIcebergPartitionSpec.toPartitionSpec(schema, partitioning))
            .withWriteOrder(ToIcebergSortOrder.toSortOrder(schema, sortOrders));
    return builder.build();
  }

  /**
   * Transforms the gravitino distribution to the distribution mode name of the Iceberg table.
   *
   * @param distribution The distribution of the table.
   * @return The distribution mode name of the iceberg table.
   */
  @VisibleForTesting
  String transformDistribution(Distribution distribution) {
    switch (distribution.strategy()) {
      case HASH:
        Preconditions.checkArgument(
            ArrayUtils.isEmpty(distribution.expressions()),
            "Iceberg's Distribution Mode.HASH does not support set expressions.");
        Preconditions.checkArgument(
            ArrayUtils.isNotEmpty(partitioning),
            "Iceberg's Distribution Mode.HASH is distributed based on partition, but the partition is empty.");
        return DistributionMode.HASH.modeName();
      case RANGE:
        Preconditions.checkArgument(
            ArrayUtils.isEmpty(distribution.expressions()),
            "Iceberg's Distribution Mode.RANGE not support set expressions.");
        Preconditions.checkArgument(
            ArrayUtils.isNotEmpty(partitioning) || ArrayUtils.isNotEmpty(sortOrders),
            "Iceberg's Distribution Mode.RANGE is distributed based on sortOrder or partition, but both are empty.");
        return DistributionMode.RANGE.modeName();
      case NONE:
        return DistributionMode.NONE.modeName();
      default:
        throw new IllegalArgumentException(
            "Iceberg unsupported distribution strategy: " + distribution.strategy());
    }
  }

  /**
   * Creates a new IcebergTable instance from a Table and a Builder.
   *
   * @param table The inner Table representing the IcebergTable.
   * @param tableName The name of Table.
   * @return A new IcebergTable instance.
   */
  public static IcebergTable fromIcebergTable(TableMetadata table, String tableName) {
    Map<String, String> properties = new HashMap<>(table.properties());
    properties.putAll(buildReservedProperties(table));
    Schema schema = table.schema();
    Transform[] partitionSpec = FromIcebergPartitionSpec.fromPartitionSpec(table.spec(), schema);
    SortOrder[] sortOrder = FromIcebergSortOrder.fromSortOrder(table.sortOrder());
    Distribution distribution = Distributions.NONE;
    String distributionName = properties.get(DISTRIBUTION_MODE);
    if (null != distributionName) {
      switch (DistributionMode.fromName(distributionName)) {
        case HASH:
          distribution = Distributions.HASH;
          break;
        case RANGE:
          distribution = Distributions.RANGE;
          break;
        default:
          // do nothing
          break;
      }
    }
    IcebergColumn[] icebergColumns =
        schema.columns().stream().map(ConvertUtil::fromNestedField).toArray(IcebergColumn[]::new);
    return IcebergTable.builder()
        .withComment(table.property(IcebergTablePropertiesMetadata.COMMENT, null))
        .withLocation(table.location())
        .withProperties(properties)
        .withColumns(icebergColumns)
        .withName(tableName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withPartitioning(partitionSpec)
        .withSortOrders(sortOrder)
        .withDistribution(distribution)
        .build();
  }

  @Override
  protected TableOperations newOps() {
    // todo: implement this method when we have the Iceberg table operations.
    throw new UnsupportedOperationException("IcebergTable does not support TableOperations.");
  }

  /** A builder class for constructing IcebergTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, IcebergTable> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    private String location;

    public Builder withLocation(String location) {
      this.location = location;
      return this;
    }

    /**
     * Internal method to build an IcebergTable instance using the provided values.
     *
     * @return A new IcebergTable instance with the configured values.
     */
    @Override
    protected IcebergTable internalBuild() {
      IcebergTable icebergTable = new IcebergTable();
      icebergTable.name = name;
      icebergTable.comment = comment;
      icebergTable.properties =
          properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      icebergTable.auditInfo = auditInfo;
      icebergTable.columns = columns;
      if (null != location) {
        icebergTable.location = location;
      } else {
        icebergTable.location = icebergTable.properties.get(PROP_LOCATION);
      }
      icebergTable.partitioning = partitioning;
      icebergTable.distribution = distribution;
      icebergTable.sortOrders = sortOrders;
      if (null != comment) {
        icebergTable.properties.putIfAbsent(ICEBERG_COMMENT_FIELD_NAME, comment);
      }
      return icebergTable;
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

  private static Map<String, String> buildReservedProperties(TableMetadata table) {
    Map<String, String> properties = new HashMap<>();
    String fileFormat =
        table
            .properties()
            .getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    properties.put(FORMAT, String.join("/", DEFAULT_ICEBERG_PROVIDER, fileFormat));
    properties.put(PROVIDER, DEFAULT_ICEBERG_PROVIDER);
    String currentSnapshotId =
        table.currentSnapshot() != null
            ? String.valueOf(table.currentSnapshot().snapshotId())
            : "none";
    properties.put(CURRENT_SNAPSHOT_ID, currentSnapshotId);
    properties.put(LOCATION, table.location());

    properties.put(FORMAT_VERSION, String.valueOf(table.formatVersion()));

    if (table.sortOrder().isUnsorted()) {
      properties.put(SORT_ORDER, describeIcebergSortOrder(table.sortOrder()));
    }

    Set<String> identifierFields = table.schema().identifierFieldNames();
    if (!identifierFields.isEmpty()) {
      properties.put(IDENTIFIER_FIELDS, "[" + String.join(",", identifierFields) + "]");
    }

    return properties;
  }

  private static String describeIcebergSortOrder(org.apache.iceberg.SortOrder sortOrder) {
    return Joiner.on(", ")
        .join(SortOrderVisitor.visit(sortOrder, DescribeIcebergSortOrderVisitor.INSTANCE));
  }
}
