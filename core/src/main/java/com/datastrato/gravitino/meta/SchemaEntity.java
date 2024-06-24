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
import java.util.Collections;
import java.util.Map;
import lombok.ToString;

/** A class representing a schema entity in Gravitino. */
@ToString
public class SchemaEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID = Field.required("id", Long.class, "The schema's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The schema's name");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the schema");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the schema");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the schema");

  private AuditInfo auditInfo;

  protected Namespace namespace;

  private EntityMetadata entityMetadata = new EntityMetadata(null, null, null, null);

  private SchemaEntity() {}

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns an unmodifiable map of the fields and their corresponding values for this schema.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, entityMetadata.getId());
    fields.put(NAME, entityMetadata.getName());
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(COMMENT, entityMetadata.getComment());
    fields.put(PROPERTIES, entityMetadata.getProperties());

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the schema.
   *
   * @return The name of the schema.
   */
  @Override
  public String name() {
    return entityMetadata.getName();
  }

  /**
   * Returns the unique id of the schema.
   *
   * @return The unique id of the schema.
   */
  @Override
  public Long id() {
    return entityMetadata.getId();
  }

  /**
   * Returns the namespace of the schema.
   *
   * @return The namespace of the schema.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the audit details of the schema.
   *
   * @return The audit details of the schema.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the comment of the schema. The returned string can be null if it is not stored in the
   * Gravitino storage.
   *
   * @return The comment of the schema.
   */
  public String comment() {
    return entityMetadata.getComment();
  }

  /**
   * Return the properties of the schema. The returned map can be null if it is not stored in the
   * Gravitino storage.
   *
   * @return The properties of the schema.
   */
  public Map<String, String> properties() {
    return entityMetadata.getProperties();
  }

  /**
   * Returns the type of the entity, which is {@link EntityType#SCHEMA}.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.SCHEMA;
  }

  // Ignore field namespace
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchemaEntity)) {
      return false;
    }
    SchemaEntity schema = (SchemaEntity) o;
    return Objects.equal(entityMetadata.getId(), schema.entityMetadata.getId())
        && Objects.equal(entityMetadata.getName(), schema.entityMetadata.getName())
        && Objects.equal(namespace, schema.namespace)
        && Objects.equal(entityMetadata.getComment(), schema.entityMetadata.getComment())
        && Objects.equal(entityMetadata.getProperties(), schema.entityMetadata.getProperties())
        && Objects.equal(auditInfo, schema.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        entityMetadata.getId(),
        entityMetadata.getName(),
        auditInfo,
        entityMetadata.getComment(),
        entityMetadata.getProperties());
  }

  /** A builder class for {@link SchemaEntity}. */
  public static class Builder {

    private final SchemaEntity schema;

    private Builder() {
      this.schema = new SchemaEntity();
    }

    /**
     * Sets the unique identifier of the schema.
     *
     * @param id The unique identifier of the schema.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      schema.entityMetadata.setId(id);
      return this;
    }

    /**
     * Sets the name of the schema.
     *
     * @param name The name of the schema.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      schema.entityMetadata.setName(name);
      return this;
    }

    /**
     * Sets the namespace of the schema.
     *
     * @param namespace The namespace of the schema.
     * @return The builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      schema.namespace = namespace;
      return this;
    }

    /**
     * Sets the comment of the schema.
     *
     * @param comment The comment of the schema.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      schema.entityMetadata.setComment(comment);
      return this;
    }

    /**
     * Sets the properties of the schema.
     *
     * @param properties The properties of the schema.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      schema.entityMetadata.setProperties(properties);
      return this;
    }

    /**
     * Sets the audit details of the schema.
     *
     * @param auditInfo The audit details of the schema.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      schema.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds the instance of the schema with the provided attributes.
     *
     * @return The built schema instance.
     */
    public SchemaEntity build() {
      schema.validate();
      return schema;
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
}
