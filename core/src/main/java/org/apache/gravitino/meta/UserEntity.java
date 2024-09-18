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
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.User;

/** A class representing a user metadata entity in Apache Gravitino. */
@ToString
public class UserEntity implements User, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, " The unique id of the user entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the user entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the user entity.");

  public static final Field ROLE_NAMES =
      Field.optional("role_names", List.class, "The role names of the user entity");

  public static final Field ROLE_IDS =
      Field.optional("role_ids", List.class, "The role ids of the user entity");

  private Long id;
  private String name;
  private AuditInfo auditInfo;
  private List<String> roleNames;
  private List<Long> roleIds;
  private Namespace namespace;

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
    fields.put(ROLE_NAMES, roleNames);
    fields.put(ROLE_IDS, roleIds);

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
   * Returns the namespace of the user.
   *
   * @return The namespace of the user.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the audit details of the user.
   *
   * @return The audit details of the user.
   */
  @Override
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
    return roleNames;
  }

  /**
   * Returns the role names of the user entity.
   *
   * @return The role names of the user entity.
   */
  public List<String> roleNames() {
    return roleNames;
  }

  /**
   * Returns the role ids of the user entity.
   *
   * @return The role ids of the user entity.
   */
  public List<Long> roleIds() {
    return roleIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UserEntity)) return false;

    UserEntity that = (UserEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
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
    private final UserEntity userEntity;

    private Builder() {
      this.userEntity = new UserEntity();
    }

    /**
     * Sets the unique id of the user entity.
     *
     * @param id The unique id of the user entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      userEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the user entity.
     *
     * @param name The name of the user entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      userEntity.name = name;
      return this;
    }

    /**
     * Sets the audit details of the user entity.
     *
     * @param auditInfo The audit details of the user entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      userEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the role names of the user entity.
     *
     * @param roles The role names of the user entity.
     * @return The builder instance.
     */
    public Builder withRoleNames(List<String> roles) {
      userEntity.roleNames = roles;
      return this;
    }

    /**
     * Sets the role ids of the user entity.
     *
     * @param roleIds The role ids of the user entity.
     * @return The builder instance.
     */
    public Builder withRoleIds(List<Long> roleIds) {
      userEntity.roleIds = roleIds;
      return this;
    }

    /**
     * Sets the namespace of the user entity.
     *
     * @param namespace The namespace of the user entity.
     * @return The builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      userEntity.namespace = namespace;
      return this;
    }

    /**
     * Builds the user entity.
     *
     * @return The built user entity.
     */
    public UserEntity build() {
      userEntity.validate();
      return userEntity;
    }
  }
}
