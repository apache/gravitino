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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.policy.Policy;

@ToString
public class PolicyEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the policy entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the policy entity.");
  public static final Field TYPE =
      Field.required("type", String.class, "The type of the policy entity.");
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
      Field.required("content", Policy.Content.class, "The content of the policy entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the policy entity.");

  public static Builder builder() {
    return new Builder();
  }

  private Long id;
  private String name;
  private Namespace namespace;
  private String type;
  private String comment;
  private boolean enabled;
  private boolean exclusive;
  private boolean inheritable;
  private Set<MetadataObject.Type> supportedObjectTypes;
  private Policy.Content content;
  private AuditInfo auditInfo;

  private PolicyEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(TYPE, type);
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

  public String policyType() {
    return type;
  }

  public String comment() {
    return comment;
  }

  public boolean enabled() {
    return enabled;
  }

  public boolean exclusive() {
    return exclusive;
  }

  public boolean inheritable() {
    return inheritable;
  }

  public Set<MetadataObject.Type> supportedObjectTypes() {
    return supportedObjectTypes;
  }

  public Policy.Content content() {
    return content;
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
        && Objects.equals(type, that.type)
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
        type,
        comment,
        enabled,
        exclusive,
        inheritable,
        supportedObjectTypes,
        content,
        auditInfo);
  }

  public static class Builder {
    private Long id;
    private String name;
    private Namespace namespace;
    private String type;
    private String comment;
    private boolean enabled = true;
    private boolean exclusive;
    private boolean inheritable;
    private Set<MetadataObject.Type> supportedObjectTypes;
    private Policy.Content content;
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

    public Builder withType(String type) {
      this.type = type;
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

    public Builder withContent(Policy.Content content) {
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
      policyEntity.type = type;
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
