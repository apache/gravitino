/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergSortOrder;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ToIcebergPartitionSpec;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.converter.ToIcebergSortOrder;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper;
import com.datastrato.gravitino.catalog.rel.BaseTable;
import com.datastrato.gravitino.meta.AuditInfo;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
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

    Map<String, String> resultProperties =
        Maps.newHashMap(IcebergTableOpsHelper.removeReservedProperties(properties));
    resultProperties.putIfAbsent(ICEBERG_COMMENT_FIELD_NAME, comment);
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .withName(name)
            .withLocation(location)
            .withSchema(schema)
            .setProperties(resultProperties)
            .withPartitionSpec(ToIcebergPartitionSpec.toPartitionSpec(schema, partitions))
            .withWriteOrder(ToIcebergSortOrder.toSortOrder(schema, sortOrders));
    return builder.build();
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
    IcebergColumn[] icebergColumns =
        schema.columns().stream().map(ConvertUtil::fromNestedField).toArray(IcebergColumn[]::new);
    return new IcebergTable.Builder()
        .withComment(table.property(IcebergTablePropertiesMetadata.COMMENT, ""))
        .withLocation(table.location())
        .withProperties(properties)
        .withColumns(icebergColumns)
        .withName(tableName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withPartitions(FromIcebergPartitionSpec.fromPartitionSpec(table.spec(), schema))
        .withSortOrders(FromIcebergSortOrder.fromSortOrder(table.sortOrder()))
        .build();
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
      icebergTable.partitions = partitions;
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
