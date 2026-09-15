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

package org.apache.gravitino.catalog.fluss;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.ToString;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

/** Represents an Apache Fluss table as a Gravitino {@link Table}. */
@ToString(exclude = "adminOps")
public class FlussTable extends BaseTable {

  private FlussAdminOps adminOps;
  private TablePath tablePath;
  private List<String> partitionKeys;

  /**
   * Creates a builder for {@link FlussTable}.
   *
   * @return the table builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Converts Gravitino table metadata to a Fluss table descriptor.
   *
   * @param columns the Gravitino columns
   * @param comment the table comment
   * @param properties the table properties
   * @param partitions the table partition transforms
   * @param distribution the table distribution
   * @param sortOrders the table sort orders
   * @param indexes the table indexes
   * @return the converted Fluss table descriptor
   */
  public static TableDescriptor toTableDescriptor(
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    if (sortOrders != null && sortOrders.length > 0) {
      throw new IllegalArgumentException("Fluss does not support sort orders");
    }

    TableDescriptor.Builder builder = TableDescriptor.builder();
    builder.schema(toFlussSchema(columns, indexes));
    builder.properties(FlussMetadataUtils.tableOptions(properties));
    builder.customProperties(FlussMetadataUtils.internalProperties(properties));

    if (comment != null) {
      builder.comment(comment);
    }

    List<String> partitionKeys = partitionKeys(partitions);
    if (!partitionKeys.isEmpty()) {
      builder.partitionedBy(partitionKeys);
    }

    applyDistribution(builder, distribution);
    return builder.build();
  }

  /**
   * Converts Fluss table info to a Gravitino table.
   *
   * @param tableInfo the Fluss table info
   * @return the converted Gravitino table
   */
  public static FlussTable fromTableInfo(TableInfo tableInfo) {
    Schema schema = tableInfo.getSchema();
    Column[] columns =
        schema.getColumns().stream().map(FlussColumn::fromFlussColumn).toArray(Column[]::new);

    Transform[] partitioning;
    if (tableInfo.isPartitioned()) {
      partitioning =
          tableInfo.getPartitionKeys().stream().map(Transforms::identity).toArray(Transform[]::new);
    } else {
      partitioning = new Transform[0];
    }

    Distribution distribution;
    if (tableInfo.hasBucketKey()) {
      NamedReference[] bucketRefs =
          tableInfo.getBucketKeys().stream()
              .map(NamedReference::field)
              .toArray(NamedReference[]::new);
      distribution = Distributions.hash(tableInfo.getNumBuckets(), bucketRefs);
    } else if (tableInfo.getNumBuckets() > 0) {
      distribution = Distributions.hash(tableInfo.getNumBuckets());
    } else {
      distribution = Distributions.NONE;
    }

    Map<String, String> properties = new LinkedHashMap<>(tableInfo.getProperties().toMap());
    properties.putAll(tableInfo.getCustomProperties().toMap());

    return FlussTable.builder()
        .withName(tableInfo.getTablePath().getTableName())
        .withComment(tableInfo.getComment().orElse(null))
        .withProperties(properties)
        .withColumns(columns)
        .withPartitioning(partitioning)
        .withDistribution(distribution)
        .withIndexes(toIndexes(schema))
        .withAuditInfo(
            FlussMetadataUtils.toAuditInfo(tableInfo.getCreatedTime(), tableInfo.getModifiedTime()))
        .build();
  }

  void initOpsContext(FlussAdminOps adminOps, TablePath tablePath, List<String> partitionKeys) {
    this.adminOps = adminOps;
    this.tablePath = tablePath;
    this.partitionKeys =
        partitionKeys == null ? Collections.emptyList() : List.copyOf(partitionKeys);
  }

  @Override
  public SupportsPartitions supportPartitions() {
    return (SupportsPartitions) ops();
  }

  @Override
  protected TableOperations newOps() throws UnsupportedOperationException {
    Preconditions.checkNotNull(
        adminOps, "Fluss admin ops must be set before creating TableOperations");
    return new FlussTableOperations(adminOps, tablePath, partitionKeys);
  }

  /** A builder class for constructing {@link FlussTable} instances. */
  public static class Builder extends BaseTableBuilder<Builder, FlussTable> {

    @Override
    protected FlussTable internalBuild() {
      FlussTable table = new FlussTable();
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.auditInfo = auditInfo;
      table.columns = columns;
      table.partitioning = partitioning;
      table.sortOrders = sortOrders;
      table.distribution = distribution;
      table.indexes = indexes;
      table.proxyPlugin = proxyPlugin;
      return table;
    }
  }

  private static Schema toFlussSchema(Column[] columns, Index[] indexes) {
    List<Schema.Column> flussColumns =
        Arrays.stream(columns).map(FlussColumn::toFlussColumn).collect(Collectors.toList());
    Schema.Builder builder = Schema.newBuilder().fromColumns(flussColumns);

    Arrays.stream(columns)
        .filter(Column::autoIncrement)
        .map(Column::name)
        .forEach(builder::enableAutoIncrement);

    Optional<Index> primaryKey = primaryKey(indexes);
    primaryKey.ifPresent(
        index ->
            builder.primaryKeyNamed(
                index.name() == null ? Indexes.DEFAULT_PRIMARY_KEY_NAME : index.name(),
                Arrays.stream(index.fieldNames())
                    .map(FlussMetadataUtils::requireTopLevelField)
                    .collect(Collectors.toList())));

    return builder.build();
  }

  private static Index[] toIndexes(Schema schema) {
    return schema
        .getPrimaryKey()
        .map(
            primaryKey ->
                new Index[] {
                  Indexes.primary(
                      primaryKey.getConstraintName(),
                      primaryKey.getColumnNames().stream()
                          .map(name -> new String[] {name})
                          .toArray(String[][]::new))
                })
        .orElse(Indexes.EMPTY_INDEXES);
  }

  private static Optional<Index> primaryKey(Index[] indexes) {
    if (indexes == null || indexes.length == 0) {
      return Optional.empty();
    }

    List<Index> primaryKeys =
        Arrays.stream(indexes)
            .filter(index -> index.type() == Index.IndexType.PRIMARY_KEY)
            .toList();
    if (primaryKeys.size() > 1) {
      throw new IllegalArgumentException("Fluss only supports one primary key");
    }

    Arrays.stream(indexes)
        .filter(index -> index.type() != Index.IndexType.PRIMARY_KEY)
        .findFirst()
        .ifPresent(
            index -> {
              throw new IllegalArgumentException("Fluss only supports primary key indexes");
            });

    return primaryKeys.stream().findFirst();
  }

  private static List<String> partitionKeys(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return Collections.emptyList();
    }

    return Arrays.stream(partitions).map(FlussTable::toPartitionKey).collect(Collectors.toList());
  }

  private static String toPartitionKey(Transform partition) {
    if (!Transforms.NAME_OF_IDENTITY.equals(partition.name())
        || !(partition instanceof Transform.SingleFieldTransform)) {
      throw new IllegalArgumentException("Fluss only supports identity partition transforms");
    }

    return FlussMetadataUtils.requireTopLevelField(
        ((Transform.SingleFieldTransform) partition).fieldName());
  }

  private static void applyDistribution(
      TableDescriptor.Builder builder, Distribution distribution) {
    if (distribution == null || distribution.strategy() == Strategy.NONE) {
      return;
    }

    if (distribution.strategy() != Strategy.HASH) {
      throw new IllegalArgumentException("Fluss only supports HASH or NONE distribution");
    }

    List<String> bucketKeys =
        Arrays.stream(distribution.expressions())
            .map(FlussTable::toDistributionField)
            .collect(Collectors.toList());
    Integer bucketCount =
        distribution.number() == Distributions.AUTO ? null : distribution.number();
    builder.distributedBy(bucketCount, bucketKeys);
  }

  private static String toDistributionField(Expression expression) {
    if (!(expression instanceof NamedReference)) {
      throw new IllegalArgumentException("Fluss only supports field references in distribution");
    }
    return FlussMetadataUtils.requireTopLevelField(((NamedReference) expression).fieldName());
  }
}
