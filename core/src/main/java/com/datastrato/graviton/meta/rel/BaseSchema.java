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
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;

/** An abstract class representing a base schema in a relational database. */
@ToString
public class BaseSchema implements Schema, Entity, HasIdentifier {

  public static final Field ID = Field.required("id", Long.class, "The schema's unique identifier");
  public static final Field CATALOG_ID =
      Field.required("catalog_id", Long.class, "The catalog's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The schema's name");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description for the schema");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The associated properties of the schema");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the schema");

  @Getter protected Long id;

  @Getter protected Long catalogId;

  protected String name;

  @Nullable @Getter protected String comment;

  @Nullable @Getter protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Namespace namespace;

  /**
   * Returns an unmodifiable map of the fields and their corresponding values for this schema.
   *
   * @return An unmodifiable map of the fields and values.
   */
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

  /**
   * Returns the name of the schema.
   *
   * @return The name of the schema.
   */
  @Override
  public String name() {
    return name;
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
   * Returns the comment or description for the schema.
   *
   * @return The comment or description for the schema.
   */
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Returns the associated properties of the schema.
   *
   * @return The associated properties of the schema.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
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
   * Returns the type of the entity, which is {@link EntityType#SCHEMA}.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.SCHEMA;
  }

  // Ignore field namespace and comment
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseSchema schema = (BaseSchema) o;
    return Objects.equal(id, schema.id)
        && Objects.equal(catalogId, schema.catalogId)
        && Objects.equal(name, schema.name)
        && Objects.equal(properties, schema.properties)
        && Objects.equal(auditInfo, schema.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, catalogId, name, properties, auditInfo);
  }

  /**
   * Builder interface for creating instances of {@link BaseSchema}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the schema being built.
   */
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

  /**
   * An abstract class implementing the builder interface for {@link BaseSchema}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the schema being built.
   */
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

    /**
     * Sets the unique identifier of the schema.
     *
     * @param id The unique identifier of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withId(Long id) {
      this.id = id;
      return self();
    }

    /**
     * Sets the unique identifier of the catalog.
     *
     * @param catalogId The unique identifier of the catalog.
     * @return The builder instance.
     */
    @Override
    public SELF withCatalogId(Long catalogId) {
      this.catalogId = catalogId;
      return self();
    }

    /**
     * Sets the name of the schema.
     *
     * @param name The name of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the namespace of the schema.
     *
     * @param namespace The namespace of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return self();
    }

    /**
     * Sets the comment of the schema.
     *
     * @param comment The comment or description for the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the associated properties of the schema.
     *
     * @param properties The associated properties of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit details of the schema.
     *
     * @param auditInfo The audit details of the schema.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the instance of the schema with the provided attributes.
     *
     * @return The built schema instance.
     */
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

  public static class SchemaBuilder extends BaseSchemaBuilder<SchemaBuilder, BaseSchema> {

    @Override
    protected BaseSchema internalBuild() {
      BaseSchema baseSchema = new BaseSchema();
      baseSchema.id = id;
      baseSchema.catalogId = catalogId;
      baseSchema.name = name;
      baseSchema.comment = comment;
      baseSchema.properties = properties;
      baseSchema.auditInfo = auditInfo;

      return baseSchema;
    }
  }
}
