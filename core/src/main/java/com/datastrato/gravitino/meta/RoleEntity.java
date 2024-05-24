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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
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
      Field.required("securable_objects", List.class, "The securable objects of the role entity.");

  private static final Splitter DOT = Splitter.on('.');

  private Long id;
  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private Namespace namespace;
  private List<SecurableObject> securableObjects;

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
   * @return The securable objects of the role.
   */
  @Override
  public List<SecurableObject> securableObjects() {
    // The securable object is a special kind of entities. Some entity types aren't the securable
    // object, such as
    // User, Role, etc.
    // The securable object identifier must be unique.
    // Gravitino assumes that the identifiers of the entities may be the same if they have different
    // types.
    // So one type of them can't be the securable object at least if there are the two same
    // identifier
    // entities .
    return securableObjects;
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
    fields.put(SECURABLE_OBJECT, securableObjects);

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
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(properties, that.properties)
        && Objects.equals(securableObjects, that.securableObjects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, properties, auditInfo, securableObjects);
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

  public boolean hasPrivilegeWithCondition(
      SecurableObject targetObject, Privilege.Name targetPrivilege, Privilege.Condition condition) {
    String metalake = namespace.level(0);
    for (SecurableObject object : securableObjects) {
      if (isSameOrParent(
          convertInnerObject(metalake, object), convertInnerObject(metalake, targetObject))) {
        for (Privilege privilege : object.privileges()) {
          if (privilege.name().equals(targetPrivilege) && privilege.condition().equals(condition)) {
            return true;
          }
        }
      }
    }
    return false;
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
     * Sets the securable objects of the role entity.
     *
     * @param securableObjects The securable objects of the role entity.
     * @return The builder instance.
     */
    public Builder withSecurableObjects(List<SecurableObject> securableObjects) {
      roleEntity.securableObjects = ImmutableList.copyOf(securableObjects);
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

  private static InnerSecurableObject convertInnerObject(
      String metalake, SecurableObject securableObject) {
    if (SecurableObject.Type.METALAKE.equals(securableObject.type())) {
      if ("*".equals(securableObject.name())) {
        return ROOT;
      } else {
        return new InnerSecurableObject(ROOT, securableObject.name());
      }
    }

    InnerSecurableObject currentObject = new InnerSecurableObject(ROOT, metalake);
    List<String> names = DOT.splitToList(securableObject.fullName());
    for (String name : names) {
      currentObject = new InnerSecurableObject(currentObject, name);
    }
    return currentObject;
  }

  private static final InnerSecurableObject ROOT = new InnerSecurableObject(null, "*");

  private static class InnerSecurableObject {
    InnerSecurableObject parent;
    String name;

    InnerSecurableObject(InnerSecurableObject parent, String name) {
      this.parent = parent;
      this.name = name;
    }

    public InnerSecurableObject getParent() {
      return parent;
    }

    public String getName() {
      return name;
    }


    @Override
    public int hashCode() {
      return Objects.hash(parent, name);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof InnerSecurableObject)) {
        return false;
      }

      InnerSecurableObject innerSecurableObject = (InnerSecurableObject) obj;
      return Objects.equals(this.name, innerSecurableObject.name)
          && Objects.equals(this.parent, innerSecurableObject.parent);
    }
  }

  private static boolean isSameOrParent(InnerSecurableObject object, InnerSecurableObject target) {
    if (object.equals(target)) {
      return true;
    }

    if (target.getParent() != null) {
      return isSameOrParent(object, target.getParent());
    }

    return false;
  }
}
