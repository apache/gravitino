/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.Namespace;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.ToString;

/** A class representing a table entity in Gravitino. */
@ToString
public class TableEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID = Field.required("id", Long.class, "The table's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The table's name");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the table");

  private Long id;

  private String name;

  private AuditInfo auditInfo;

  private Namespace namespace;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableEntity)) {
      return false;
    }

    // Ignore field namespace
    TableEntity baseTable = (TableEntity) o;
    return Objects.equal(id, baseTable.id)
        && Objects.equal(name, baseTable.name)
        && Objects.equal(auditInfo, baseTable.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, auditInfo);
  }

  public static class Builder {

    private final TableEntity tableEntity;

    public Builder() {
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

    public TableEntity build() {
      tableEntity.validate();
      return tableEntity;
    }
  }
}
