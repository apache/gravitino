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
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.IdpGroup;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.utils.CollectionUtils;

/** A class representing a built-in IdP group metadata entity in Apache Gravitino. */
@ToString
public class IdpGroupEntity implements IdpGroup, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the built-in IdP group entity.");

  public static final Field NAME =
      Field.required("name", String.class, "The name of the built-in IdP group entity.");

  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the built-in IdP group.");

  public static final Field USER_NAMES =
      Field.optional("user_names", List.class, "The user names of the built-in IdP group.");

  private Long id;
  private String name;
  private AuditInfo auditInfo;
  private List<String> userNames;
  private Namespace namespace;

  private IdpGroupEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(USER_NAMES, userNames);
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
  public EntityType type() {
    return EntityType.IDP_GROUP;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public List<String> userNames() {
    return userNames;
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
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(auditInfo, that.auditInfo)
        && CollectionUtils.isEqualCollection(userNames, that.userNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, auditInfo, userNames);
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
     * Sets the audit details of the built-in IdP group entity.
     *
     * @param auditInfo The audit details of the built-in IdP group entity.
     * @return The builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      groupEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the user names of the built-in IdP group entity.
     *
     * @param userNames The user names of the built-in IdP group entity.
     * @return The builder instance.
     */
    public Builder withUserNames(List<String> userNames) {
      groupEntity.userNames = userNames;
      return this;
    }

    /**
     * Sets the namespace of the built-in IdP group entity.
     *
     * @param namespace The namespace of the built-in IdP group entity.
     * @return The builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      groupEntity.namespace = namespace;
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
