/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.meta;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;
import org.apache.gravitino.Field;
import org.apache.gravitino.connector.GenericLakehouseColumn;
import org.apache.gravitino.connector.GenericLakehouseTable;
import org.apache.gravitino.rel.GenericTable;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

@Getter
public class GenericTableEntity extends TableEntity {
  public static final Field FORMAT = Field.required("format", Long.class, "The table's format");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The table's properties");

  public static final Field PARTITIONS =
      Field.optional("partitions", Transform[].class, "The table's partition");

  public static final Field SORT_ORDER =
      Field.optional("sortOrders", SortOrder[].class, "The table's sort order");

  public static final Field DISTRIBUTION =
      Field.optional("distribution", Distribution.class, "The table's distribution");

  public static final Field INDEXES =
      Field.optional("indexes", Index[].class, "The table's indexes");

  public static final Field COMMENT =
      Field.optional("comment", String.class, "The table's comment");

  public GenericTableEntity() {
    super();
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> superFields = super.fields();
    Map<Field, Object> result = Maps.newHashMap(superFields);
    result.put(FORMAT, format);
    result.put(PROPERTIES, properties);
    result.put(PARTITIONS, partitions);
    result.put(SORT_ORDER, sortOrder);
    result.put(DISTRIBUTION, distribution);
    result.put(INDEXES, indexes);
    result.put(COMMENT, comment);

    return result;
  }

  private String format;
  @Getter private Map<String, String> properties;
  private Transform[] partitions;
  private SortOrder[] sortOrder;
  private Distribution distribution;
  private Index[] indexes;
  private String comment;

  public static class Builder {
    private final GenericTableEntity tableEntity;

    public Builder() {
      this.tableEntity = new GenericTableEntity();
    }

    public Builder withId(Long id) {
      tableEntity.id = id;
      return this;
    }

    public Builder withName(String name) {
      tableEntity.name = name;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      tableEntity.auditInfo = auditInfo;
      return this;
    }

    public Builder withColumns(java.util.List<ColumnEntity> columns) {
      tableEntity.columns = columns;
      return this;
    }

    public Builder withNamespace(org.apache.gravitino.Namespace namespace) {
      tableEntity.namespace = namespace;
      return this;
    }

    public Builder withFormat(String format) {
      tableEntity.format = format;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      tableEntity.properties = properties;
      return this;
    }

    public Builder withPartitions(Transform[] partitions) {
      tableEntity.partitions = partitions;
      return this;
    }

    public Builder withSortOrder(SortOrder[] sortOrder) {
      tableEntity.sortOrder = sortOrder;
      return this;
    }

    public Builder withDistribution(Distribution distribution) {
      tableEntity.distribution = distribution;
      return this;
    }

    public Builder withIndexes(Index[] indexes) {
      tableEntity.indexes = indexes;
      return this;
    }

    public Builder withComment(String comment) {
      tableEntity.comment = comment;
      return this;
    }

    public GenericTableEntity build() {
      return tableEntity;
    }
  }

  public static GenericTableEntity.Builder getBuilder() {
    return new GenericTableEntity.Builder();
  }

  public GenericTable toGenericTable() {
    return GenericLakehouseTable.builder()
        .withFormat(format)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .withSortOrders(sortOrder)
        .withPartitioning(partitions)
        .withDistribution(distribution)
        .withColumns(
            columns.stream()
                .map(this::toGenericLakehouseColumn)
                .toArray(GenericLakehouseColumn[]::new))
        .withIndexes(indexes)
        .withName(name)
        .withComment(comment)
        .build();
  }

  private GenericLakehouseColumn toGenericLakehouseColumn(ColumnEntity columnEntity) {
    return GenericLakehouseColumn.builder()
        .withName(columnEntity.name())
        .withComment(columnEntity.comment())
        .withAutoIncrement(columnEntity.autoIncrement())
        .withNullable(columnEntity.nullable())
        .withType(columnEntity.dataType())
        .withDefaultValue(columnEntity.defaultValue())
        .build();
  }
}
