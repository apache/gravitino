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
import com.datastrato.gravitino.authorization.Group;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GroupEntity implements Group, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the group entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the group entity.");

  public static final Field ROLE_NAMES =
      Field.optional("roleNames", List.class, "The role names of the group entity.");

  public static final Field ROLE_IDS =
      Field.optional("roleIds", List.class, "The role names of the group entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the group entity.");

  private Long id;
  private String name;
  private AuditInfo auditInfo;
  private List<String> roleNames;
  private List<Long> roleIds;
  private Namespace namespace;

  private GroupEntity() {}

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
    fields.put(ROLE_NAMES, roleNames);
    fields.put(ROLE_IDS, roleIds);

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
   * Returns the namespace of the group.
   *
   * @return The namespace of the group.
   */
  @Override
  public Namespace namespace() {
    return namespace;
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
   * Returns the roles of the group entity.
   *
   * @return The roles of the group entity.
   */
  public List<String> roles() {
    return roleNames;
  }

  /**
   * Returns the role ids of the group entity.
   *
   * @return The roles of the group entity.
   */
  public List<Long> roleIds() {
    return roleIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GroupEntity)) return false;

    GroupEntity that = (GroupEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(roleNames, that.roleNames)
        && Objects.equals(roleIds, that.roleIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, auditInfo, roleNames, roleIds);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final GroupEntity groupEntity;

    private Builder() {
      this.groupEntity = new GroupEntity();
    }

    /**
     * Sets the unique id of the group entity.
     *
     * @param id The unique id of the group entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      groupEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the group entity.
     *
     * @param name The name of the group entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      groupEntity.name = name;
      return this;
    }

    /**
     * Sets the audit details of the group entity.
     *
     * @param auditInfo The audit details of the group entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      groupEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the role names of the group entity.
     *
     * @param roles The role names of the group entity.
     * @return The builder instance.
     */
    public Builder withRoleNames(List<String> roles) {
      groupEntity.roleNames = roles;
      return this;
    }

    /**
     * Sets the role ids of the group entity.
     *
     * @param roleIds The role names of the group entity.
     * @return The builder instance.
     */
    public Builder withRoleIds(List<Long> roleIds) {
      groupEntity.roleIds = roleIds;
      return this;
    }

    /**
     * Sets the namespace of the group entity.
     *
     * @param namespace The namespace of the group entity.
     * @return The builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      groupEntity.namespace = namespace;
      return this;
    }

    /**
     * Builds the group entity.
     *
     * @return The built group entity.
     */
    public GroupEntity build() {
      groupEntity.validate();
      return groupEntity;
    }
  }
}
