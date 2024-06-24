/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.tag.Tag;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TagEntity implements Tag, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the tag entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the tag entity.");

  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the tag entity.");

  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the tag entity.");

  public static final Field ASSOCIATED_OBJECTS =
      Field.optional(
          "objects", MetadataObject[].class, "The associated objects of the tag entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", Audit.class, "The audit details of the tag entity.");

  private Namespace namespace;
  private MetadataObject[] objects = null;
  private Audit auditInfo;
  private EntityMetadata entityMetadata = new EntityMetadata(null, null, null, null);

  private TagEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, entityMetadata.getId());
    fields.put(NAME, entityMetadata.getName());
    fields.put(COMMENT, entityMetadata.getComment());
    fields.put(PROPERTIES, entityMetadata.getProperties());
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(ASSOCIATED_OBJECTS, objects);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public EntityType type() {
    return EntityType.TAG;
  }

  @Override
  public Long id() {
    return entityMetadata.getId();
  }

  @Override
  public String name() {
    return entityMetadata.getName();
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public String comment() {
    return entityMetadata.getComment();
  }

  @Override
  public Map<String, String> properties() {
    return entityMetadata.getProperties();
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  public MetadataObject[] objects() {
    return objects;
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagEntity)) {
      return false;
    }

    TagEntity that = (TagEntity) o;
    return Objects.equals(entityMetadata.getId(), that.entityMetadata.getId())
        && Objects.equals(entityMetadata.getName(), that.entityMetadata.getName())
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(entityMetadata.getComment(), that.entityMetadata.getComment())
        && Objects.equals(entityMetadata.getProperties(), that.entityMetadata.getProperties())
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        entityMetadata.getId(),
        entityMetadata.getName(),
        namespace,
        entityMetadata.getComment(),
        entityMetadata.getProperties(),
        auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final TagEntity tagEntity;

    private Builder() {
      this.tagEntity = new TagEntity();
    }

    public Builder withId(Long id) {
      tagEntity.entityMetadata.setId(id);
      return this;
    }

    public Builder withName(String name) {
      tagEntity.entityMetadata.setName(name);
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      tagEntity.namespace = namespace;
      return this;
    }

    public Builder withComment(String comment) {
      tagEntity.entityMetadata.setComment(comment);
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      tagEntity.entityMetadata.setProperties(properties);
      return this;
    }

    public Builder withMetadataObjects(MetadataObject[] objects) {
      tagEntity.objects = objects;
      return this;
    }

    public Builder withAuditInfo(Audit auditInfo) {
      tagEntity.auditInfo = auditInfo;
      return this;
    }

    public TagEntity build() {
      tagEntity.validate();
      return tagEntity;
    }
  }
}
