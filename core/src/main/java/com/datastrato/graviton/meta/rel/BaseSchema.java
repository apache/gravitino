/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Schema;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public abstract class BaseSchema implements Schema, Entity, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the schema");
  public static final Field CATALOG_ID =
      Field.required("catalog_id", Long.class, "The unique identifier of the catalog");
  public static final Field NAME = Field.required("name", String.class, "The name of the schema");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the schema");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the schema");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the schema");

  protected Long id;

  protected Long catalogId;

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Namespace namespace;

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(CATALOG_ID, catalogId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseSchema> {
    SELF withId(Long id);

    SELF withCatalogId(Long catalogId);

    SELF withName(String name);

    SELF withNamespace(Namespace namespace);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public abstract static class BaseSchemaBuilder<
          SELF extends Builder<SELF, T>, T extends BaseSchema>
      implements Builder<SELF, T> {
    protected Long id;
    protected Long catalogId;
    protected String name;
    protected Namespace namespace;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;

    @Override
    public SELF withId(Long id) {
      this.id = id;
      return self();
    }

    @Override
    public SELF withCatalogId(Long catalogId) {
      this.catalogId = catalogId;
      return self();
    }

    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    @Override
    public SELF withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return self();
    }

    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
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
