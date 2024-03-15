/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.Group;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MetalakeGroup implements Group, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the group entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the group entity.");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the group entity.");

  public static final Field USERS =
      Field.required("users", List.class, "The users of the group entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the group entity.");

  private Long id;
  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private String metalake;
  private List<String> users;

  private MetalakeGroup() {}

  /**
   * Returns a map of fields and their corresponding values for this group entity.
   *
   * @return An unmodifiable map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(USERS, users);
    fields.put(PROPERTIES, properties);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the name of the group.
   *
   * @return The name of the group.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique id of the group.
   *
   * @return The unique id of the group.
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
    return EntityType.GROUP;
  }

  /**
   * Returns the audit details of the group.
   *
   * @return The audit details of the group.
   */
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the properties of the group entity.
   *
   * @return The properties of the group entity.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the users of the group entity.
   *
   * @return The users of the group entity.
   */
  public List<String> users() {
    return users;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MetalakeGroup)) return false;

    MetalakeGroup that = (MetalakeGroup) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties)
        && Objects.equals(users, that.users);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, properties, auditInfo, users);
  }

  @Override
  public NameIdentifier nameIdentifier() {
    return NameIdentifier.of(metalake, name);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final MetalakeGroup metalakeGroup;

    private Builder() {
      this.metalakeGroup = new MetalakeGroup();
    }

    /**
     * Sets the unique id of the group entity.
     *
     * @param id The unique id of the group entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      metalakeGroup.id = id;
      return this;
    }

    /**
     * Sets the name of the group entity.
     *
     * @param name The name of the group entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      metalakeGroup.name = name;
      return this;
    }

    /**
     * Sets the properties of the group entity.
     *
     * @param properties The properties of the group entity.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      metalakeGroup.properties = properties;
      return this;
    }

    /**
     * Sets the audit details of the group entity.
     *
     * @param auditInfo The audit details of the group entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      metalakeGroup.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the metalake of the group entity.
     *
     * @param metalake The metalake of the group entity.
     * @return The builder instance.
     */
    public Builder withMetalake(String metalake) {
      metalakeGroup.metalake = metalake;
      return this;
    }

    /**
     * Sets the users of the group entity.
     *
     * @param users The users of the group entity.
     * @return The builder instance.
     */
    public Builder withUsers(List<String> users) {
      metalakeGroup.users = users;
      return this;
    }

    /**
     * Builds the group entity.
     *
     * @return The built group entity.
     */
    public MetalakeGroup build() {
      metalakeGroup.validate();
      return metalakeGroup;
    }
  }
}
