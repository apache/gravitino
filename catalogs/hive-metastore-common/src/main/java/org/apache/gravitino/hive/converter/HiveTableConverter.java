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
package org.apache.gravitino.hive.converter;

import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.connector.BaseColumn;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveTableConverter {
  public static AuditInfo getAuditInfo(Table table) {
    // Get audit info from Hive's Table object. Because Hive's table doesn't store last modifier
    // and last modified time, we only get creator and create time from Hive's table.
    AuditInfo.Builder auditInfoBuilder = AuditInfo.builder();
    Optional.ofNullable(table.getOwner()).ifPresent(auditInfoBuilder::withCreator);
    if (table.isSetCreateTime()) {
      auditInfoBuilder.withCreateTime(Instant.ofEpochSecond(table.getCreateTime()));
    }
    return auditInfoBuilder.build();
  }

  public static Distribution getDistribution(Table table) {
    StorageDescriptor sd = table.getSd();
    Distribution distribution = Distributions.NONE;
    if (sd.getBucketCols() != null && !sd.getBucketCols().isEmpty()) {
      // Hive table use hash strategy as bucketing strategy
      distribution =
          Distributions.hash(
              sd.getNumBuckets(),
              sd.getBucketCols().stream().map(NamedReference::field).toArray(Expression[]::new));
    }
    return distribution;
  }

  public static SortOrder[] getSortOrders(Table table) {
    SortOrder[] sortOrders = SortOrders.NONE;
    StorageDescriptor sd = table.getSd();
    if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
      sortOrders =
          sd.getSortCols().stream()
              .map(
                  f ->
                      SortOrders.of(
                          NamedReference.field(f.getCol()),
                          f.getOrder() == 1 ? SortDirection.ASCENDING : SortDirection.DESCENDING))
              .toArray(SortOrder[]::new);
    }
    return sortOrders;
  }

  public static Transform[] getPartitioning(Table table) {
    return table.getPartitionKeys().stream()
        .map(p -> identity(p.getName()))
        .toArray(Transform[]::new);
  }

  public static <
          BUILDER extends BaseColumn.BaseColumnBuilder<BUILDER, COLUMN>, COLUMN extends BaseColumn>
      Column[] getColumns(Table table, BUILDER columnBuilder) {
    StorageDescriptor sd = table.getSd();
    // Collect column names from sd.getCols() to check for duplicates
    Set<String> columnNames =
        sd.getCols().stream().map(FieldSchema::getName).collect(Collectors.toSet());

    return Stream.concat(
            sd.getCols().stream()
                .map(
                    f ->
                        columnBuilder
                            .withName(f.getName())
                            .withType(HiveDataTypeConverter.CONVERTER.toGravitino(f.getType()))
                            .withComment(f.getComment())
                            .build()),
            table.getPartitionKeys().stream()
                // Filter out partition keys that already exist in sd.getCols()
                .filter(p -> !columnNames.contains(p.getName()))
                .map(
                    p ->
                        columnBuilder
                            .withName(p.getName())
                            .withType(HiveDataTypeConverter.CONVERTER.toGravitino(p.getType()))
                            .withComment(p.getComment())
                            .build()))
        .toArray(Column[]::new);
  }
}
