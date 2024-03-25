/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Field;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.User;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;

/** A class representing a user metadata entity in Gravitino. */
@ToString
public class ManagedUser implements User, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the user entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the user entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the user entity.");

  public static final Field GROUPS =
      Field.optional("groups", List.class, "The groups of the user entity");

  public static final Field ROLES =
      Field.optional("roles", List.class, "The roles of the user entity");

  private Long id;
  private String name;
  private AuditInfo auditInfo;
  private String metalake;
  private List<String> groups;
  private List<String> roles;

  private ManagedUser() {}

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
    fields.put(ROLES, roles);
    fields.put(GROUPS, groups);

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
   * The roles of the user.
   *
   * @return The roles of the user.
   */
  @Override
  public List<String> roles() {
    return roles;
  }

  /**
   * The groups of the user.
   *
   * @return The groups of the user.
   */
  @Override
  public List<String> groups() {
    return groups;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ManagedUser)) return false;

    ManagedUser that = (ManagedUser) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(roles, that.roles)
        && Objects.equals(groups, that.groups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, auditInfo, roles, groups);
  }

  @Override
  public NameIdentifier nameIdentifier() {
    return NameIdentifier.of(metalake, name);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ManagedUser managedUser;

    private Builder() {
      this.managedUser = new ManagedUser();
    }

    /**
     * Sets the unique id of the user entity.
     *
     * @param id The unique id of the user entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      managedUser.id = id;
      return this;
    }

    /**
     * Sets the name of the user entity.
     *
     * @param name The name of the user entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      managedUser.name = name;
      return this;
    }

    /**
     * Sets the audit details of the user entity.
     *
     * @param auditInfo The audit details of the user entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      managedUser.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the metalake of the user entity.
     *
     * @param metalake The metalake of the user entity.
     * @return The builder instance.
     */
    public Builder withMetalake(String metalake) {
      managedUser.metalake = metalake;
      return this;
    }

    /**
     * Sets the roles of the user entity.
     *
     * @param roles The roles of the user entity.
     * @return The builder instance.
     */
    public Builder withRoles(List<String> roles) {
      managedUser.roles = roles;
      return this;
    }

    /**
     * Sets the groups of the user entity.
     *
     * @param groups The groups of the user entity.
     * @return The builder instance.
     */
    public Builder withGroups(List<String> groups) {
      managedUser.groups = groups;
      return this;
    }

    /**
     * Builds the user entity.
     *
     * @return The built user entity.
     */
    public ManagedUser build() {
      managedUser.validate();
      return managedUser;
    }
  }
}
