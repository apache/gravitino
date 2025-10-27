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
package org.apache.gravitino.meta;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.CollectionUtils;

/** A class representing a table entity in Apache Gravitino. */
@ToString
public class TableEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID = Field.required("id", Long.class, "The table's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The table's name");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the table");
  public static final Field COLUMNS =
      Field.optional("columns", List.class, "The columns of the table");

  public static final Field FORMAT = Field.optional("format", String.class, "The table's format");
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

  private Long id;

  private String name;

  private AuditInfo auditInfo;

  private Namespace namespace;

  private List<ColumnEntity> columns;

  @Getter private String format;
  @Getter private Map<String, String> properties;
  @Getter private Transform[] partitions;
  @Getter private SortOrder[] sortOrder;
  @Getter private Distribution distribution;
  @Getter private Index[] indexes;
  @Getter private String comment;

  /**
   * Returns a map of the fields and their corresponding values for this table.
   *
   * @return A map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(COLUMNS, columns);

    fields.put(FORMAT, format);
    fields.put(PROPERTIES, properties);
    fields.put(PARTITIONS, partitions);
    fields.put(SORT_ORDER, sortOrder);
    fields.put(DISTRIBUTION, distribution);
    fields.put(INDEXES, indexes);
    fields.put(COMMENT, comment);

    return fields;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.TABLE;
  }

  /**
   * Returns the audit details of the table.
   *
   * @return The audit details of the table.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the name of the table.
   *
   * @return The name of the table.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique id of the table.
   *
   * @return The unique id of the table.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the namespace of the table.
   *
   * @return The namespace of the table.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  public List<ColumnEntity> columns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableEntity)) {
      return false;
    }

    TableEntity baseTable = (TableEntity) o;
    return Objects.equal(id, baseTable.id)
        && Objects.equal(name, baseTable.name)
        && Objects.equal(namespace, baseTable.namespace)
        && Objects.equal(auditInfo, baseTable.auditInfo)
        && CollectionUtils.isEqualCollection(columns, baseTable.columns)
        && Objects.equal(format, baseTable.format)
        // Please check the correctness of this comparison.
        && Objects.equal(properties, baseTable.properties)
        && Arrays.equals(partitions, baseTable.partitions)
        && Arrays.equals(sortOrder, baseTable.sortOrder)
        && Objects.equal(distribution, baseTable.distribution)
        && Arrays.equals(indexes, baseTable.indexes)
        && Objects.equal(comment, baseTable.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, auditInfo, columns, namespace);
  }

  public static class Builder {

    private final TableEntity tableEntity;

    private Builder() {
      this.tableEntity = new TableEntity();
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

    public Builder withNamespace(Namespace namespace) {
      tableEntity.namespace = namespace;
      return this;
    }

    public Builder withColumns(List<ColumnEntity> columns) {
      tableEntity.columns = columns;
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

    public TableEntity build() {
      tableEntity.validate();

      if (tableEntity.columns == null) {
        tableEntity.columns = Collections.emptyList();
      }

      if (tableEntity.properties == null) {
        tableEntity.properties = Collections.emptyMap();
      }

      if (tableEntity.indexes == null) {
        tableEntity.indexes = new Index[0];
      }

      if (tableEntity.partitions == null) {
        tableEntity.partitions = new Transform[0];
      }

      if (tableEntity.sortOrder == null) {
        tableEntity.sortOrder = new SortOrder[0];
      }

      return tableEntity;
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
}
