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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

public class GlueTable implements Table {
  private static final org.slf4j.Logger LOG =
          org.slf4j.LoggerFactory.getLogger(GlueTable.class);
  private final String name;
  private final String comment;
  private final Column[] columns;
  private final Map<String, String> properties;
  private final AuditInfo auditInfo;
  private final Transform[] partitioning;

  private GlueTable(
          String name,
          String comment,
          Column[] columns,
          Transform[] partitioning,
          Map<String, String> properties,
          AuditInfo auditInfo) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.partitioning = partitioning;
    this.properties = properties;
    this.auditInfo = auditInfo;
  }

  private static org.apache.gravitino.rel.types.Type convertType(String colName, String glueType) {
    try {
      return GlueTypeConverter.fromGlue(glueType);
    } catch (IllegalArgumentException e) {
      LOG.warn(
              "Unknown Glue type '{}' for column '{}', defaulting to string",
              glueType,
              colName);
      return Types.StringType.get();
    }
  }

  public static GlueTable fromGlueTable(
          software.amazon.awssdk.services.glue.model.Table glueTable, NameIdentifier ident) {

    StorageDescriptor sd = glueTable.storageDescriptor();

    // Regular (non-partition) columns from the storage descriptor
    List<Column> cols = new ArrayList<>();
    if (sd != null && sd.columns() != null) {
      sd.columns().stream()
              .map(col -> Column.of(col.name(), convertType(col.name(), col.type()), col.comment()))
              .forEach(cols::add);
    }

    // Partition columns — append after regular columns
    List<software.amazon.awssdk.services.glue.model.Column> partitionKeys =
            glueTable.partitionKeys() != null ? glueTable.partitionKeys() : Collections.emptyList();

    for (software.amazon.awssdk.services.glue.model.Column pk : partitionKeys) {
      cols.add(Column.of(pk.name(), convertType(pk.name(), pk.type()), pk.comment()));
    }

    Column[] columns = cols.toArray(new Column[0]);

    // Build identity transforms for each partition key
    Transform[] partitioning =
            partitionKeys.stream()
                    .map(
                            pk ->
                                    org.apache.gravitino.rel.expressions.transforms.Transforms.identity(pk.name()))
                    .toArray(Transform[]::new);

    AuditInfo auditInfo =
            AuditInfo.builder()
                    .withCreator("glue")
                    .withCreateTime(
                            glueTable.createTime() != null ? glueTable.createTime() : Instant.now())
                    .build();

    Map<String, String> properties =
            glueTable.parameters() != null ? glueTable.parameters() : Collections.emptyMap();

    return new GlueTable(
            glueTable.name(), glueTable.description(), columns, partitioning, properties, auditInfo);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Transform[] partitioning() { return partitioning; }

  @Override
  public Distribution distribution() {
    return Distributions.NONE;
  }

  @Override
  public SortOrder[] sortOrder() {
    return new SortOrder[0];
  }

  @Override
  public AuditInfo auditInfo() { return auditInfo; }


}

