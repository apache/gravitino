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
import com.datastrato.gravitino.authorization.Resource;
import com.datastrato.gravitino.authorization.Resources;
import com.datastrato.gravitino.authorization.Role;
import com.google.common.collect.Lists;
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

  public static final Field RESOURCE_IDENTIFIER =
      Field.required(
          "resource_identifier",
          NameIdentifier.class,
          "The resource identifier of the role entity.");

  public static final Field RESOURCE_TYPE =
      Field.required(
          "resource_entity_type", EntityType.class, "The resource type of the role entity.");

  public static final Field PRIVILEGES =
      Field.required("privileges", List.class, "The privileges of the role entity.");

  private Long id;
  private String name;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private NameIdentifier resourceIdentifier;

  private EntityType resourceType;
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
   * The properties of the role. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the role.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * The name of the role contains one resource. For example: If the resource is a table, the
   * identifier may be `catalog1.schema1.table1`.
   *
   * @return The resource of the role.
   */
  @Override
  public Resource resource() {
    // The resource is a special kind of entities. The resource identifier must be unique.
    // Gravitino assumes that the names of the entities may be the same if they have different
    // types.
    // So one of them can't be the resource at least.
    List<String> names = Lists.newArrayList();
    String[] levels = resourceIdentifier.namespace().levels();
    for (int i = 1; i < levels.length; i++) {
      names.add(levels[i]);
    }
    names.add(resourceIdentifier.name());

    return Resources.of(names.toArray(new String[0]));
  }

  /**
   * The privilege entity type of the role. For example: If the entity is a table, the type will be
   * TABLE.
   *
   * @return The privilege entity type of the role.
   */
  public String resourceType() {
    return resourceType.toString();
  }

  public NameIdentifier resourceEntityIdentifier() {
    return resourceIdentifier;
  }

  /**
   * The privileges of the role. All privileges belong to one resource. For example: If the resource
   * is a table, the privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a schema has the
   * privilege of `LOAD TABLE`. It means the role can all tables of the schema.
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
    fields.put(RESOURCE_IDENTIFIER, resourceIdentifier);
    fields.put(RESOURCE_TYPE, resourceType);
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
        && Objects.equals(resourceIdentifier, that.resourceIdentifier)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(privileges, that.privileges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, name, properties, auditInfo, resourceIdentifier, resourceType, privileges);
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
     * Sets the resource identifier of the role entity.
     *
     * @param resource The resource identifier of the role entity.
     * @return The builder instance.
     */
    public Builder withResourceIdentifier(NameIdentifier resource) {
      roleEntity.resourceIdentifier = resource;
      return this;
    }

    /**
     * Sets the resource type of the role entity.
     *
     * @param type The resource type of the role entity.
     * @return The builder instance.
     */
    public Builder withResourceType(EntityType type) {
      roleEntity.resourceType = type;
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
