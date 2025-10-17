/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.meta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.utils.CollectionUtils;

public class RoleEntity implements Role, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the role entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the role entity.");

  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the role entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the role entity.");

  public static final Field SECURABLE_OBJECTS =
      Field.optional("securable_objects", List.class, "The securable objects of the role entity.");

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

  /**
   * @return The audit information of the entity.
   */
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
    fields.put(SECURABLE_OBJECTS, securableObjects);

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
        && CollectionUtils.isEqualCollection(securableObjects, that.securableObjects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, properties, auditInfo, securableObjects, namespace);
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
}
