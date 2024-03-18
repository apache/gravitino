/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.DISTRIBUTION_MODE;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.requests.CreateTableRequest;

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

  public static final String ICEBERG_COMMENT_FIELD_NAME = "comment";

  private String location;

  private IcebergTable() {}

  public CreateTableRequest toCreateTableRequest() {
    Schema schema = ConvertUtil.toIcebergSchema(this);
    properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
    properties.put(DISTRIBUTION_MODE, transformDistribution(distribution));
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .withName(name)
            .withLocation(location)
            .withSchema(schema)
            .setProperties(properties)
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
    Map<String, String> properties = table.properties();
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
    return new IcebergTable.Builder()
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
      String provider = icebergTable.properties.get(PROP_PROVIDER);
      if (provider != null && !DEFAULT_ICEBERG_PROVIDER.equalsIgnoreCase(provider)) {
        throw new IllegalArgumentException("Unsupported format in USING: " + provider);
      }
      return icebergTable;
    }
  }
}
