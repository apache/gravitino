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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergSortOrder;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.ToIcebergPartitionSpec;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.ToIcebergSortOrder;
import org.apache.gravitino.catalog.lakehouse.iceberg.utils.IcebergTablePropertiesUtil;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.requests.CreateTableRequest;

/** Represents an Apache Iceberg Table entity in the Iceberg table. */
@ToString
@Getter
public class IcebergTable extends BaseTable implements SupportsPartitionSpecs {

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

  /**
   * All Iceberg partition specs keyed by {@code spec-id}. Populated from metadata in {@link
   * #fromIcebergTable}, or from the current {@code partitioning} (spec {@code 0}) in {@link
   * Builder#internalBuild()}.
   */
  @Getter(AccessLevel.NONE)
  private Map<Integer, Transform[]> icebergPartitionSpecs = ImmutableMap.of();

  /** Iceberg default partition spec ID for this table metadata. */
  @Getter(AccessLevel.NONE)
  private int icebergDefaultSpecId;

  private IcebergTable() {}

  /** {@inheritDoc} */
  @Override
  public SupportsPartitionSpecs supportsPartitionSpecs() {
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public int defaultPartitionSpecId() {
    return icebergDefaultSpecId;
  }

  /** {@inheritDoc} */
  @Override
  public Map<Integer, Transform[]> partitionSpecs() {
    return copyPartitionSpecs(icebergPartitionSpecs);
  }

  public static Map<String, String> rebuildCreateProperties(Map<String, String> createProperties) {
    // Gravitino owns the default Iceberg table format version: when it is not explicitly set, stamp
    // ICEBERG_DEFAULT_FORMAT_VERSION rather than relying on the Iceberg library's version-dependent
    // default or letting a blank value fail to parse downstream.
    String formatVersion = createProperties.get(IcebergTablePropertiesMetadata.FORMAT_VERSION);
    if (StringUtils.isBlank(formatVersion)) {
      createProperties.put(
          IcebergTablePropertiesMetadata.FORMAT_VERSION,
          String.valueOf(IcebergTablePropertiesMetadata.ICEBERG_DEFAULT_FORMAT_VERSION));
    }

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
    properties.put(
        IcebergTablePropertiesMetadata.DISTRIBUTION_MODE, transformDistribution(distribution));
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
   * Transforms the Gravitino distribution to the distribution mode name of the Iceberg table.
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
    properties.putAll(IcebergTablePropertiesUtil.buildReservedProperties(table));
    Schema schema = table.schema();
    Transform[] partitionSpec = FromIcebergPartitionSpec.fromPartitionSpec(table.spec(), schema);
    SortOrder[] sortOrder = FromIcebergSortOrder.fromSortOrder(table.sortOrder());
    IcebergColumn[] icebergColumns =
        schema.columns().stream().map(ConvertUtil::fromNestedField).toArray(IcebergColumn[]::new);
    Map<Integer, Transform[]> allSpecs = new TreeMap<>();
    for (PartitionSpec spec : table.specsById().values()) {
      allSpecs.put(
          spec.specId(), copyTransforms(FromIcebergPartitionSpec.fromPartitionSpec(spec, schema)));
    }
    Preconditions.checkState(
        allSpecs.containsKey(table.defaultSpecId()),
        "Iceberg default partition spec id %s is missing from specs %s",
        table.defaultSpecId(),
        allSpecs.keySet());
    IcebergTable icebergTable =
        IcebergTable.builder()
            .withComment(table.property(IcebergTablePropertiesMetadata.COMMENT, null))
            .withLocation(table.location())
            .withProperties(properties)
            .withColumns(icebergColumns)
            .withName(tableName)
            .withAuditInfo(AuditInfo.EMPTY)
            .withPartitioning(partitionSpec)
            .withSortOrders(sortOrder)
            .withDistribution(getDistribution(properties))
            .build();
    icebergTable.icebergPartitionSpecs = ImmutableMap.copyOf(allSpecs);
    icebergTable.icebergDefaultSpecId = table.defaultSpecId();
    return icebergTable;
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
      Transform[] normalizedPartitioning =
          partitioning != null ? partitioning : Transforms.EMPTY_TRANSFORM;
      icebergTable.partitioning = normalizedPartitioning;
      icebergTable.distribution = distribution;
      icebergTable.sortOrders = sortOrders;
      // Iceberg always retains at least spec 0 (unpartitioned has empty fields). Hydrate a
      // matching default until fromIcebergTable replaces this with full metadata.
      icebergTable.icebergPartitionSpecs =
          ImmutableMap.of(0, copyTransforms(normalizedPartitioning));
      icebergTable.icebergDefaultSpecId = 0;
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

  private static Transform[] copyTransforms(Transform[] transforms) {
    if (transforms == null || transforms.length == 0) {
      return Transforms.EMPTY_TRANSFORM;
    }
    return Arrays.copyOf(transforms, transforms.length);
  }

  private static Map<Integer, Transform[]> copyPartitionSpecs(
      Map<Integer, Transform[]> partitionSpecs) {
    Map<Integer, Transform[]> copied = new TreeMap<>();
    for (Map.Entry<Integer, Transform[]> entry : partitionSpecs.entrySet()) {
      copied.put(entry.getKey(), copyTransforms(entry.getValue()));
    }
    return ImmutableMap.copyOf(copied);
  }

  private static Distribution getDistribution(Map<String, String> properties) {
    Distribution distribution = Distributions.NONE;
    String distributionName = properties.get(IcebergTablePropertiesMetadata.DISTRIBUTION_MODE);
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
    return distribution;
  }
}
