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
package org.apache.gravitino.catalog.glue;

import static org.apache.gravitino.catalog.glue.GlueConstants.INPUT_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.LOCATION;
import static org.apache.gravitino.catalog.glue.GlueConstants.OUTPUT_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.SERDE_LIB;
import static org.apache.gravitino.catalog.glue.GlueConstants.SERDE_NAME;
import static org.apache.gravitino.catalog.glue.GlueConstants.SERDE_PARAMETER_PREFIX;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Represents an AWS Glue {@link Table} as a Gravitino table.
 *
 * <p>All entries in {@code Table.parameters()} pass through intact (including {@code table_type},
 * {@code metadata_location}, etc.), so downstream tools can correctly identify the table format.
 * StorageDescriptor fields (location, formats, SerDe) are surfaced as additional properties.
 */
@ToString
public class GlueTable extends BaseTable {

  private GlueTable() {}

  @Override
  protected TableOperations newOps() {
    // Partition operations are deferred to PR-06.
    throw new UnsupportedOperationException(
        "Partition operations are not yet supported for GlueTable");
  }

  /**
   * Converts an AWS Glue {@link Table} to a {@link GlueTable}.
   *
   * <p>Column assembly:
   *
   * <ol>
   *   <li>Data columns from {@code storageDescriptor.columns()} (Hive-format tables).
   *   <li>Partition columns from {@code table.partitionKeys()} appended after data columns.
   * </ol>
   *
   * <p>For Iceberg-format tables the StorageDescriptor columns are typically empty; all metadata
   * (including {@code table_type=ICEBERG} and {@code metadata_location}) is in {@code
   * table.parameters()} and passes through as-is.
   *
   * @param glueTable the Glue Table returned by the AWS SDK
   * @return a populated {@link GlueTable}
   */
  public static GlueTable fromGlueTable(Table glueTable) {
    StorageDescriptor sd = glueTable.storageDescriptor();

    // --- Columns ---
    List<Column> columns = new ArrayList<>();
    if (sd != null && sd.hasColumns()) {
      for (software.amazon.awssdk.services.glue.model.Column c : sd.columns()) {
        columns.add(GlueColumn.fromGlueColumn(c));
      }
    }
    List<String> partitionColNames = new ArrayList<>();
    if (glueTable.hasPartitionKeys()) {
      for (software.amazon.awssdk.services.glue.model.Column pk : glueTable.partitionKeys()) {
        columns.add(GlueColumn.fromGlueColumn(pk));
        partitionColNames.add(pk.name());
      }
    }

    // --- Partitioning ---
    Transform[] partitioning =
        partitionColNames.stream().map(Transforms::identity).toArray(Transform[]::new);

    // --- Distribution (bucket) ---
    Distribution distribution = Distributions.NONE;
    if (sd != null && sd.hasBucketColumns() && sd.numberOfBuckets() > 0) {
      distribution =
          Distributions.hash(
              sd.numberOfBuckets(),
              sd.bucketColumns().stream()
                  .map(NamedReference::field)
                  .toArray(org.apache.gravitino.rel.expressions.Expression[]::new));
    }

    // --- Sort orders ---
    SortOrder[] sortOrders = SortOrders.NONE;
    if (sd != null && sd.hasSortColumns()) {
      sortOrders =
          sd.sortColumns().stream()
              .map(
                  o ->
                      SortOrders.of(
                          NamedReference.field(o.column()),
                          o.sortOrder() == 1 ? SortDirection.ASCENDING : SortDirection.DESCENDING))
              .toArray(SortOrder[]::new);
    }

    // --- Properties ---
    Map<String, String> properties = new HashMap<>();
    if (glueTable.hasParameters()) {
      properties.putAll(glueTable.parameters());
    }
    if (StringUtils.isNotBlank(glueTable.tableType())) {
      properties.put(TABLE_TYPE, glueTable.tableType());
    }
    if (sd != null) {
      putIfNotBlank(properties, LOCATION, sd.location());
      putIfNotBlank(properties, INPUT_FORMAT, sd.inputFormat());
      putIfNotBlank(properties, OUTPUT_FORMAT, sd.outputFormat());
      if (sd.serdeInfo() != null) {
        putIfNotBlank(properties, SERDE_LIB, sd.serdeInfo().serializationLibrary());
        putIfNotBlank(properties, SERDE_NAME, sd.serdeInfo().name());
        if (sd.serdeInfo().parameters() != null) {
          sd.serdeInfo()
              .parameters()
              .forEach((k, v) -> properties.put(SERDE_PARAMETER_PREFIX + k, v));
        }
      }
    }

    // --- AuditInfo ---
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreateTime(glueTable.createTime())
            .withLastModifiedTime(glueTable.updateTime())
            .build();

    return GlueTable.builder()
        .withName(glueTable.name())
        .withComment(glueTable.description())
        .withColumns(columns.toArray(new Column[0]))
        .withProperties(properties)
        .withPartitioning(partitioning)
        .withDistribution(distribution)
        .withSortOrders(sortOrders)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static void putIfNotBlank(Map<String, String> map, String key, String value) {
    if (StringUtils.isNotBlank(value)) {
      map.put(key, value);
    }
  }

  /** Builder for {@link GlueTable}. */
  public static class Builder extends BaseTableBuilder<Builder, GlueTable> {

    private Builder() {}

    @Override
    protected GlueTable internalBuild() {
      GlueTable table = new GlueTable();
      table.name = name;
      table.comment = comment;
      table.columns = columns;
      table.properties = properties;
      table.partitioning = partitioning;
      table.sortOrders = sortOrders;
      table.distribution = distribution;
      table.indexes = indexes;
      table.auditInfo = auditInfo;
      table.proxyPlugin = Optional.empty();
      return table;
    }
  }

  /**
   * Creates a new {@link Builder}.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }
}
