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
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.utils.CollectionUtils;

/** A class representing a built-in IdP group metadata entity in Apache Gravitino. */
@ToString
public class IdpGroupEntity implements IdpGroup, IdpEntity {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the built-in IdP group entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the built-in IdP group entity.");

  public static final Field USERNAMES =
      Field.optional("usernames", List.class, "The usernames of the built-in IdP group.");

  private Long id;
  private String name;
  private List<String> usernames;

  private IdpGroupEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(USERNAMES, usernames);
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
    return IdpEntityType.IDP_GROUP;
  }

  @Override
  public List<String> usernames() {
    return usernames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IdpGroupEntity)) {
      return false;
    }

    IdpGroupEntity that = (IdpGroupEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && CollectionUtils.isEqualCollection(usernames, that.usernames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, usernames);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link IdpGroupEntity}. */
  public static class Builder {
    private final IdpGroupEntity groupEntity;

    private Builder() {
      this.groupEntity = new IdpGroupEntity();
    }

    /**
     * Sets the unique id of the built-in IdP group entity.
     *
     * @param id The unique id of the built-in IdP group entity.
     * @return The builder instance.
     */
    public Builder withId(Long id) {
      groupEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the built-in IdP group entity.
     *
     * @param name The name of the built-in IdP group entity.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      groupEntity.name = name;
      return this;
    }

    /**
     * Sets the usernames of the built-in IdP group entity.
     *
     * @param usernames The usernames of the built-in IdP group entity.
     * @return The builder instance.
     */
    public Builder withUsernames(List<String> usernames) {
      groupEntity.usernames = usernames;
      return this;
    }

    /**
     * Builds the built-in IdP group entity.
     *
     * @return The built built-in IdP group entity.
     */
    public IdpGroupEntity build() {
      groupEntity.validate();
      return groupEntity;
    }
  }
}
