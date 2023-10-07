/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.catalog.lakehouse.iceberg.converter.ConvertUtil;
import com.datastrato.graviton.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import com.datastrato.graviton.catalog.lakehouse.iceberg.converter.FromIcebergSortOrder;
import com.datastrato.graviton.catalog.lakehouse.iceberg.converter.ToIcebergPartitionSpec;
import com.datastrato.graviton.catalog.lakehouse.iceberg.converter.ToIcebergSortOrder;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper;
import com.datastrato.graviton.catalog.rel.BaseTable;
import com.datastrato.graviton.meta.AuditInfo;
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

  public static final String ICEBERG_COMMENT_FIELD_NAME = "comment";

  private String location;

  private IcebergTable() {}

  public CreateTableRequest toCreateTableRequest() {
    Schema schema = ConvertUtil.toIcebergSchema(this);

    Map<String, String> resultProperties =
        Maps.newHashMap(IcebergTableOpsHelper.removeReservedProperties(properties));
    if (null != comment) {
      resultProperties.putIfAbsent(ICEBERG_COMMENT_FIELD_NAME, comment);
    }
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
        .withComment(table.property(IcebergSchema.ICEBERG_COMMENT_FIELD_NAME, ""))
        .withLocation(table.location())
        .withProperties(properties)
        .withColumns(icebergColumns)
        .withName(tableName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withPartitions(FromIcebergPartitionSpec.formPartitionSpec(table.spec(), schema))
        .withSortOrders(FromIcebergSortOrder.formSortOrder(table.sortOrder()))
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
      icebergTable.location = location;
      icebergTable.partitions = partitions;
      icebergTable.sortOrders = sortOrders;

      return icebergTable;
    }
  }
}
