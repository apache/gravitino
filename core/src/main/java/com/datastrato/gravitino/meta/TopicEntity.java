/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.Namespace;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;

/** A class representing a topic metadata entity in Gravitino. */
@ToString
public class TopicEntity implements Entity, Auditable, HasIdentifier {
  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the topic entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the topic entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description of the topic entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the topic entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the topic entity.");
  private EntityMetadata entityMetadata = new EntityMetadata(null, null, null, null);

  public static Builder builder() {
    return new Builder();
  }

  private Namespace namespace;
  private AuditInfo auditInfo;

  private TopicEntity() {}

  /**
   * Returns a map of fields and their corresponding values for this topic entity.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, entityMetadata.getId());
    fields.put(NAME, entityMetadata.getName());
    fields.put(COMMENT, entityMetadata.getComment());
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(PROPERTIES, entityMetadata.getProperties());

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the topic.
   *
   * @return The name of the topic.
   */
  @Override
  public String name() {
    return entityMetadata.getName();
  }

  /**
   * Returns the namespace of the topic.
   *
   * @return The namespace of the topic.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the unique id of the topic.
   *
   * @return The unique id of the topic.
   */
  @Override
  public Long id() {
    return entityMetadata.getId();
  }

  /**
   * Returns the comment or description of the topic.
   *
   * @return The comment or description of the topic.
   */
  public String comment() {
    return entityMetadata.getComment();
  }

  /**
   * Returns the audit details of the topic.
   *
   * @return The audit details of the topic.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.TOPIC;
  }

  /**
   * Returns the properties of the topic entity.
   *
   * @return The properties of the topic entity.
   */
  public Map<String, String> properties() {
    return entityMetadata.getProperties();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TopicEntity)) return false;

    TopicEntity that = (TopicEntity) o;
    return Objects.equals(entityMetadata.getId(), that.entityMetadata.getId())
        && Objects.equals(entityMetadata.getName(), that.entityMetadata.getName())
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(entityMetadata.getComment(), that.entityMetadata.getComment())
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(entityMetadata.getProperties(), that.entityMetadata.getProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        entityMetadata.getId(),
        entityMetadata.getName(),
        entityMetadata.getComment(),
        auditInfo,
        entityMetadata.getProperties());
  }

  public static class Builder {
    private final TopicEntity topic;

    private Builder() {
      topic = new TopicEntity();
    }

    /**
     * Sets the unique id of the topic entity.
     *
     * @param id The unique id of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withId(Long id) {
      topic.entityMetadata.setId(id);
      return this;
    }

    /**
     * Sets the name of the topic entity.
     *
     * @param name The name of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withName(String name) {
      topic.entityMetadata.setName(name);
      return this;
    }

    /**
     * Sets the namespace of the topic entity.
     *
     * @param namespace The namespace of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withNamespace(Namespace namespace) {
      topic.namespace = namespace;
      return this;
    }

    /**
     * Sets the comment or description of the topic entity.
     *
     * @param comment The comment or description of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withComment(String comment) {
      topic.entityMetadata.setComment(comment);
      return this;
    }

    /**
     * Sets the audit details of the topic entity.
     *
     * @param auditInfo The audit details of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withAuditInfo(AuditInfo auditInfo) {
      topic.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the properties of the topic entity.
     *
     * @param properties The properties of the topic entity.
     * @return The builder instance.
     */
    public TopicEntity.Builder withProperties(Map<String, String> properties) {
      topic.entityMetadata.setProperties(properties);
      return this;
    }

    /**
     * Builds the topic entity.
     *
     * @return The built topic entity.
     */
    public TopicEntity build() {
      topic.validate();
      return topic;
    }
  }
}
