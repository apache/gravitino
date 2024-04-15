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
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.SecurableObject;
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

  public static final Field SECURABLE_OBJECT =
      Field.required(
          "securable_object", SecurableObject.class, "The securable object of the role entity.");

  public static final Field PRIVILEGES =
      Field.required("privileges", List.class, "The privileges of the role entity.");

  private Long id;
  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private List<Privilege> privileges;
  private Namespace namespace;
  private SecurableObject securableObject;

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
   * The properties of the role. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the role.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * The securable object represents a special kind of entity with a unique identifier. All
   * securable objects are organized by tree structure. For example: If the securable object is a
   * table, the identifier may be `catalog1.schema1.table1`.
   *
   * @return The securable object of the role.
   */
  @Override
  public SecurableObject securableObject() {
    // The securable object is a special kind of entities. Some entity types aren't the securable
    // object, such as
    // User, Role, etc.
    // The securable object identifier must be unique.
    // Gravitino assumes that the identifiers of the entities may be the same if they have different
    // types.
    // So one type of them can't be the securable object at least if there are the two same
    // identifier
    // entities .
    return securableObject;
  }

  /**
   * The privileges of the role. All privileges belong to one securable object. For example: If the
   * securable object is a table, the privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a
   * schema has the privilege of `LOAD TABLE`. It means the role can all tables of the schema.
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
    fields.put(SECURABLE_OBJECT, securableObject);
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
        && Objects.equals(securableObject, that.securableObject)
        && Objects.equals(privileges, that.privileges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, properties, auditInfo, securableObject, privileges);
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
     * Sets the securable object of the role entity.
     *
     * @param securableObject The securable object of the role entity.
     * @return The builder instance.
     */
    public Builder securableObject(SecurableObject securableObject) {
      roleEntity.securableObject = securableObject;
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
