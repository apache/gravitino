/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.*;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public abstract class BaseTable implements Table, Entity, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the table");
  public static final Field SCHEMA_ID =
      Field.required("schema_id", Long.class, "The unique identifier of the schema");
  public static final Field NAME = Field.required("name", String.class, "The name of the table");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the table");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the table");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the table");
  public static final Field COLUMNS =
      Field.required("columns", Column[].class, "The columns of the table");

  protected Long id;

  protected Long schemaId;

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Column[] columns;

  protected Namespace namespace;

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(SCHEMA_ID, schemaId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(COLUMNS, columns);

    return fields;
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseTable> {
    SELF withId(Long id);

    SELF withSchemaId(Long schemaId);

    SELF withNameSpace(Namespace namespace);

    SELF withName(String name);

    SELF withColumns(Column[] columns);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public abstract static class BaseTableBuilder<SELF extends Builder<SELF, T>, T extends BaseTable>
      implements Builder<SELF, T> {
    protected Long id;
    protected Long schemaId;
    protected String name;
    protected Namespace namespace;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;
    protected Column[] columns;

    @Override
    public SELF withId(Long id) {
      this.id = id;
      return self();
    }

    @Override
    public SELF withSchemaId(Long schemaId) {
      this.schemaId = schemaId;
      return self();
    }

    @Override
    public SELF withNameSpace(Namespace namespace) {
      this.namespace = namespace;
      return self();
    }

    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    @Override
    public SELF withColumns(Column[] columns) {
      this.columns = columns;
      return self();
    }

    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    @Override
    public T build() {
      T t = internalBuild();
      t.validate();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }
}
