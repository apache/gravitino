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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
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

  private Long id;

  private String name;

  private AuditInfo auditInfo;

  private Namespace namespace;

  private List<ColumnEntity> columns;

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
        && CollectionUtils.isEqualCollection(columns, baseTable.columns);
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

    public TableEntity build() {
      tableEntity.validate();

      if (tableEntity.columns == null) {
        tableEntity.columns = Collections.emptyList();
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
