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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;

@ToString
public class PolicyEntity implements Policy, Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the policy entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the policy entity.");
  public static final Field POLICY_TYPE =
      Field.required("policyType", String.class, "The type of the policy entity.");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the policy entity.");
  public static final Field ENABLED =
      Field.required("enabled", Boolean.class, "The policy entity is enabled.");
  public static final Field EXCLUSIVE =
      Field.required("exclusive", Boolean.class, "The policy entity is exclusive.");
  public static final Field INHERITABLE =
      Field.required("inheritable", Boolean.class, "The policy entity is inheritable.");
  public static final Field SUPPORTED_OBJECT_TYPES =
      Field.required(
          "supportedObjectTypes", Set.class, "The supported object types of the policy entity.");
  public static final Field CONTENT =
      Field.required("content", PolicyContent.class, "The content of the policy entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the policy entity.");

  public static Builder builder() {
    return new Builder();
  }

  private Long id;
  private String name;
  private Namespace namespace;
  private String policyType;
  private String comment;
  private boolean enabled;
  private boolean exclusive;
  private boolean inheritable;
  private Set<MetadataObject.Type> supportedObjectTypes;
  private PolicyContent content;
  private AuditInfo auditInfo;

  private PolicyEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(POLICY_TYPE, policyType);
    fields.put(COMMENT, comment);
    fields.put(ENABLED, enabled);
    fields.put(EXCLUSIVE, exclusive);
    fields.put(INHERITABLE, inheritable);
    fields.put(SUPPORTED_OBJECT_TYPES, supportedObjectTypes);
    fields.put(CONTENT, content);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public EntityType type() {
    return EntityType.POLICY;
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public String policyType() {
    return policyType;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public boolean exclusive() {
    return exclusive;
  }

  @Override
  public boolean inheritable() {
    return inheritable;
  }

  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return supportedObjectTypes;
  }

  @Override
  public PolicyContent content() {
    return content;
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Entity.super.validate();
    validatePolicy();
    content().validate();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PolicyEntity)) return false;
    PolicyEntity that = (PolicyEntity) o;
    return enabled == that.enabled
        && exclusive == that.exclusive
        && inheritable == that.inheritable
        && Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(policyType, that.policyType)
        && Objects.equals(comment, that.comment)
        && Objects.equals(supportedObjectTypes, that.supportedObjectTypes)
        && Objects.equals(content, that.content)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        name,
        namespace,
        policyType,
        comment,
        enabled,
        exclusive,
        inheritable,
        supportedObjectTypes,
        content,
        auditInfo);
  }

  private void validatePolicy() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name()), "Policy name cannot be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(policyType()), "Policy type cannot be blank");
    Preconditions.checkArgument(content() != null, "Policy content cannot be null");
    Preconditions.checkArgument(
        supportedObjectTypes() != null && !supportedObjectTypes().isEmpty(),
        "Policy must support at least one metadata object type");

    BuiltInType builtInType = BuiltInType.fromPolicyType(policyType());
    Preconditions.checkArgument(
        builtInType != BuiltInType.CUSTOM || content() instanceof PolicyContents.CustomContent,
        "Expected CustomContent for custom policy type, but got %s",
        content().getClass().getName());
    if (builtInType != BuiltInType.CUSTOM) {
      Preconditions.checkArgument(
          exclusive() == builtInType.exclusive(),
          "Expected exclusive value %s for built-in policy type %s, but got %s",
          builtInType.exclusive(),
          policyType(),
          exclusive());
      Preconditions.checkArgument(
          inheritable() == builtInType.inheritable(),
          "Expected inheritable value %s for built-in policy type %s, but got %s",
          builtInType.inheritable(),
          policyType(),
          inheritable());
      Preconditions.checkArgument(
          supportedObjectTypes().equals(builtInType.supportedObjectTypes()),
          "Expected supported object types %s for built-in policy type %s, but got %s",
          builtInType.supportedObjectTypes(),
          policyType(),
          supportedObjectTypes());
    }
  }

  public static class Builder {
    private Long id;
    private String name;
    private Namespace namespace;
    private String policyType;
    private String comment;
    private boolean enabled = true;
    private boolean exclusive;
    private boolean inheritable;
    private Set<MetadataObject.Type> supportedObjectTypes;
    private PolicyContent content;
    private AuditInfo auditInfo;

    public Builder withId(Long id) {
      this.id = id;
      return this;
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder withPolicyType(String policyType) {
      this.policyType = policyType;
      return this;
    }

    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    public Builder withEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder withExclusive(boolean exclusive) {
      this.exclusive = exclusive;
      return this;
    }

    public Builder withInheritable(boolean inheritable) {
      this.inheritable = inheritable;
      return this;
    }

    public Builder withSupportedObjectTypes(Set<MetadataObject.Type> supportedObjectTypes) {
      this.supportedObjectTypes = supportedObjectTypes;
      return this;
    }

    public Builder withContent(PolicyContent content) {
      this.content = content;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return this;
    }

    public PolicyEntity build() {
      PolicyEntity policyEntity = new PolicyEntity();
      policyEntity.id = id;
      policyEntity.name = name;
      policyEntity.namespace = namespace;
      policyEntity.policyType = policyType;
      policyEntity.comment = comment;
      policyEntity.enabled = enabled;
      policyEntity.exclusive = exclusive;
      policyEntity.inheritable = inheritable;
      policyEntity.supportedObjectTypes = supportedObjectTypes;
      policyEntity.content = content;
      policyEntity.auditInfo = auditInfo;
      policyEntity.validate();

      return policyEntity;
    }
  }
}
