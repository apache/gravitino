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

/** A class representing a user metadata entity in Gravitino. */
@ToString
public class UserEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the user entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the user entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the user entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the user entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private UserEntity() {}

  /**
   * Returns a map of fields and their corresponding values for this user entity.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(PROPERTIES, properties);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the user.
   *
   * @return The name of the user.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the namespace of the user.
   *
   * @return The namespace of the user.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the unique id of the user.
   *
   * @return The unique id of the user.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.USER;
  }

  /**
   * Returns the audit details of the user.
   *
   * @return The audit details of the user.
   */
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the properties of the user entity.
   *
   * @return The properties of the user entity.
   */
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UserEntity)) return false;

    UserEntity that = (UserEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, properties, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final UserEntity user;

    private Builder() {
      this.user = new UserEntity();
    }

    /**
     * Sets the unique id of the user entity.
     *
     * @param id The unique id of the user entity.
     * @return The builder instance.
     */
    public UserEntity.Builder withId(Long id) {
      user.id = id;
      return this;
    }

    /**
     * Sets the name of the user entity.
     *
     * @param name The name of the user entity.
     * @return The builder instance.
     */
    public UserEntity.Builder withName(String name) {
      user.name = name;
      return this;
    }

    /**
     * Sets the namespace of the user entity.
     *
     * @param namespace The namespace of the user entity.
     * @return The builder instance.
     */
    public UserEntity.Builder withNamespace(Namespace namespace) {
      user.namespace = namespace;
      return this;
    }

    /**
     * Sets the properties of the user entity.
     *
     * @param properties The properties of the user entity.
     * @return The builder instance.
     */
    public UserEntity.Builder withProperties(Map<String, String> properties) {
      user.properties = properties;
      return this;
    }

    /**
     * Sets the audit details of the user entity.
     *
     * @param auditInfo The audit details of the user entity.
     * @return The builder instance.
     */
    public UserEntity.Builder withAuditInfo(AuditInfo auditInfo) {
      user.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds the user entity.
     *
     * @return The built user entity.
     */
    public UserEntity build() {
      user.validate();
      return user;
    }
  }
}
