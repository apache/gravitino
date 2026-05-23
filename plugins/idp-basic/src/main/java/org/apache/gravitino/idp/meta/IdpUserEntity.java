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
package org.apache.gravitino.idp.meta;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Field;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.utils.CollectionUtils;

/** A class representing a built-in IdP user metadata entity in Apache Gravitino. */
@ToString
public class IdpUserEntity implements IdpUser, IdpEntity {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the built-in IdP user entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the built-in IdP user entity.");

  public static final Field GROUP_NAMES =
      Field.optional("group_names", List.class, "The group names of the built-in IdP user.");

  private Long id;
  private String name;
  private List<String> groupNames;
  private String passwordHash;

  private IdpUserEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(GROUP_NAMES, groupNames);
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public IdpEntityType type() {
    return IdpEntityType.IDP_USER;
  }

  @Override
  public List<String> groupNames() {
    return groupNames;
  }

  /**
   * Returns the password hash used when inserting the user. This value is not persisted in entity
   * fields and is only consumed by the storage layer during creation.
   *
   * @return The password hash for insert, or null if unset.
   */
  public String passwordHash() {
    return passwordHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IdpUserEntity)) {
      return false;
    }

    IdpUserEntity that = (IdpUserEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && CollectionUtils.isEqualCollection(groupNames, that.groupNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, groupNames);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link IdpUserEntity}. */
  public static class Builder {
    private final IdpUserEntity userEntity;

    private Builder() {
      this.userEntity = new IdpUserEntity();
    }

    /**
     * Sets the unique id of the built-in IdP user entity.
     *
     * @param id The unique id of the built-in IdP user entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      userEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the built-in IdP user entity.
     *
     * @param name The name of the built-in IdP user entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      userEntity.name = name;
      return this;
    }

    /**
     * Sets the group names of the built-in IdP user entity.
     *
     * @param groupNames The group names of the built-in IdP user entity.
     * @return The builder instance.
     */
    public Builder withGroupNames(List<String> groupNames) {
      userEntity.groupNames = groupNames;
      return this;
    }

    /**
     * Sets the password hash used when inserting the user.
     *
     * @param passwordHash The password hash for insert.
     * @return The builder instance.
     */
    public Builder withPasswordHash(String passwordHash) {
      userEntity.passwordHash = passwordHash;
      return this;
    }

    /**
     * Builds the built-in IdP user entity.
     *
     * @return The built built-in IdP user entity.
     */
    public IdpUserEntity build() {
      userEntity.validate();
      return userEntity;
    }
  }
}
