/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Auditable;
import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.Namespace;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class CatalogEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the catalog");
  public static final Field METALAKE_ID =
      Field.required("metalake_id", Long.class, "The unique identifier of the metalake");
  public static final Field NAME = Field.required("name", String.class, "The name of the catalog");
  public static final Field TYPE =
      Field.required("type", Catalog.Type.class, "The type of the catalog");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the catalog");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the catalog");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the catalog");

  @Getter private Long id;

  @Getter private Long metalakeId;

  private String name;

  @Getter private Catalog.Type type;

  @Nullable @Getter private String comment;

  @Nullable @Getter private Map<String, String> properties;

  private AuditInfo auditInfo;

  private Namespace namespace;

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(METALAKE_ID, metalakeId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, type);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
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
  public Namespace namespace() {
    return namespace;
  }

  public static class Builder {

    private final CatalogEntity catalog;

    public Builder() {
      catalog = new CatalogEntity();
    }

    public Builder withId(Long id) {
      catalog.id = id;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      catalog.metalakeId = metalakeId;
      return this;
    }

    public Builder withName(String name) {
      catalog.name = name;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      catalog.namespace = namespace;
      return this;
    }

    public Builder withType(Catalog.Type type) {
      catalog.type = type;
      return this;
    }

    public Builder withComment(String comment) {
      catalog.comment = comment;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      catalog.properties = properties;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      catalog.auditInfo = auditInfo;
      return this;
    }

    public CatalogEntity build() {
      catalog.validate();
      return catalog;
    }
  }
}
