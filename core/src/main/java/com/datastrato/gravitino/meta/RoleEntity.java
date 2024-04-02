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
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Role;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RoleEntity implements Role, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the role entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the role entity.");

  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the role entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the role entity.");

  public static final Field PRIVILEGE_ENTITY_IDENTIFIER =
      Field.required(
          "privilege_entity_identifier",
          NameIdentifier.class,
          "The privilege entity identifier of the role entity.");

  public static final Field PRIVILEGE_ENTITY_TYPE =
      Field.required(
          "privilege_entity_type",
          EntityType.class,
          "The privilege entity type of the role entity.");

  public static final Field PRIVILEGES =
      Field.required("privileges", List.class, "The privileges of the role entity.");

  private Long id;
  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private NameIdentifier privilegeEntityIdentifier;

  private EntityType privilegeEntityType;
  private List<Privilege> privileges;
  private Namespace namespace;

  /**
   * The name of the role.
   *
   * @return The name of the role.
   */
  @Override
  public String name() {
    return name;
  }

  /** @return The audit information of the entity. */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * The properties of the group. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the role.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * The privilege entity identifier of the role.
   *
   * @return The privilege entity identifier of the role.
   */
  @Override
  public NameIdentifier privilegeEntityIdentifier() {
    return privilegeEntityIdentifier;
  }

  /**
   * The privilege entity type of the role.
   *
   * @return The privilege entity type of the role.
   */
  @Override
  public String privilegeEntityType() {
    return privilegeEntityType.toString();
  }

  /**
   * The privileges of the role.
   *
   * @return The privileges of the role.
   */
  @Override
  public List<Privilege> privileges() {
    return privileges;
  }

  /**
   * Retrieves the fields and their associated values of the entity.
   *
   * @return A map of Field to Object representing the entity's schema with values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(PROPERTIES, properties);
    fields.put(PRIVILEGE_ENTITY_IDENTIFIER, privilegeEntityIdentifier);
    fields.put(PRIVILEGE_ENTITY_TYPE, privilegeEntityType);
    fields.put(PRIVILEGES, privileges);

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Retrieves the type of the entity.
   *
   * @return The type of the entity as defined by {@link EntityType}.
   */
  @Override
  public EntityType type() {
    return EntityType.ROLE;
  }

  /**
   * Get the unique id of the entity.
   *
   * @return The unique id of the entity.
   */
  @Override
  public Long id() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RoleEntity)) return false;

    RoleEntity that = (RoleEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties)
        && Objects.equals(privilegeEntityIdentifier, that.privilegeEntityIdentifier)
        && Objects.equals(privilegeEntityType, that.privilegeEntityType)
        && Objects.equals(privileges, that.privileges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        name,
        properties,
        auditInfo,
        privilegeEntityIdentifier,
        privilegeEntityType,
        privileges);
  }

  /**
   * Get the namespace of the entity.
   *
   * @return The namespace of the entity.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final RoleEntity roleEntity;

    private Builder() {
      roleEntity = new RoleEntity();
    }

    /**
     * Sets the unique id of the role entity.
     *
     * @param id The unique id of the role entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      roleEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the role entity.
     *
     * @param name The name of the role entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      roleEntity.name = name;
      return this;
    }

    /**
     * Sets the properties of the role entity.
     *
     * @param properties The properties of the role entity.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      roleEntity.properties = properties;
      return this;
    }

    /**
     * Sets the audit details of the role entity.
     *
     * @param auditInfo The audit details of the role entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      roleEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the privilege entity identifier of the role entity.
     *
     * @param entity The privilege entity of the role entity.
     * @return The builder instance.
     */
    public Builder withPrivilegeEntityIdentifier(NameIdentifier entity) {
      roleEntity.privilegeEntityIdentifier = entity;
      return this;
    }

    /**
     * Sets the privilege entity type of the role entity.
     *
     * @param type The privilege entity type of the role entity.
     * @return The builder instance.
     */
    public Builder withPrivilegeEntityType(EntityType type) {
      roleEntity.privilegeEntityType = type;
      return this;
    }

    /**
     * Sets the privileges of the role entity.
     *
     * @param privileges The privileges of the role entity.
     * @return The builder instance.
     */
    public Builder withPrivileges(List<Privilege> privileges) {
      roleEntity.privileges = privileges;
      return this;
    }

    /**
     * Sets the namespace of the role entity.
     *
     * @param namespace The namespace of the role entity.
     * @return The builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      roleEntity.namespace = namespace;
      return this;
    }

    /**
     * Builds the role entity.
     *
     * @return The built role entity.
     */
    public RoleEntity build() {
      roleEntity.validate();
      return roleEntity;
    }
  }
}
