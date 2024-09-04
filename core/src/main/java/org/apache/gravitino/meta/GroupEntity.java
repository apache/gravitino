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

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Group;

public class GroupEntity implements Group, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the group entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the group entity.");

  public static final Field ROLES_SUPPLIER =
      Field.required("roles_supplier", Supplier.class, "The roles supplier of the group entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the group entity.");

  private Long id;
  private String name;
  private AuditInfo auditInfo;
  private Namespace namespace;
  // The roles is a lazy field to avoid unnecessary cost.
  private Supplier<List<RoleEntity>> rolesSupplier = Collections::emptyList;

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
    fields.put(ROLES_SUPPLIER, rolesSupplier);

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
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the roles of the group entity.
   *
   * @return The roles of the group entity.
   */
  @Override
  public List<String> roles() {
    if (roleEntities() == null) {
      return null;
    }

    return roleEntities().stream().map(RoleEntity::name).collect(Collectors.toList());
  }

  /**
   * Returns the role entities of the group entity.
   *
   * @return The role entities of the group entity.
   */
  public List<RoleEntity> roleEntities() {
    return rolesSupplier.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GroupEntity)) return false;

    GroupEntity that = (GroupEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(auditInfo, that.auditInfo)
        && Objects.equals(roles(), that.roles());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, auditInfo, roles());
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
     * Sets the roles of the group entity.
     *
     * @param roles The role names of the group entity.
     * @return The builder instance.
     */
    public Builder withRoles(List<RoleEntity> roles) {
      if (roles != null) {
        groupEntity.rolesSupplier = () -> roles;
      }
      return this;
    }

    /**
     * Sets the roles supplier of the group entity.
     *
     * @param supplier The roles supplier of the group entity.
     * @return The builder instance.
     */
    public Builder withRolesSupplier(Supplier<List<RoleEntity>> supplier) {
      groupEntity.rolesSupplier = supplier;
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
