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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.IllegalPolicyException;

public class BasePolicy implements Policy {
  protected String name;

  protected String type;

  @Nullable protected String comment;

  protected boolean enabled;

  protected boolean exclusive;

  protected boolean inheritable;

  protected Set<MetadataObject.Type> supportedObjectTypes;

  protected Content content;

  protected Audit auditInfo;

  public enum ContentType {
    // Non-built-in types are all custom types.
    CUSTOM("custom", CustomPolicy.CustomContent.class);

    private final String type;
    private final Class<? extends Content> contentClass;

    ContentType(String type, Class<? extends Content> contentClass) {
      this.type = type;
      this.contentClass = contentClass;
    }

    public static ContentType fromString(String type) {
      Preconditions.checkArgument(StringUtils.isNotBlank(type), "type cannot be blank");
      for (ContentType contentType : ContentType.values()) {
        if (contentType.type.equalsIgnoreCase(type)) {
          return contentType;
        }
      }
      return CUSTOM;
    }

    public String typeString() {
      return type;
    }

    public Class<? extends Content> contentClass() {
      return contentClass;
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String type() {
    return type;
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
  public Content content() {
    return content;
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  @Override
  public void validate() throws IllegalPolicyException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "Policy name cannot be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(type), "Policy type cannot be blank");
    Preconditions.checkArgument(content() != null, "Policy content cannot be null");
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CustomPolicy)) return false;
    BasePolicy that = (BasePolicy) o;
    return enabled == that.enabled
        && exclusive == that.exclusive
        && inheritable == that.inheritable
        && Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && Objects.equals(comment, that.comment)
        && Objects.equals(supportedObjectTypes, that.supportedObjectTypes)
        && Objects.equals(content, that.content)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        type,
        comment,
        enabled,
        exclusive,
        inheritable,
        supportedObjectTypes,
        content,
        auditInfo);
  }

  public abstract static class BaseContent implements Content {
    protected Map<String, String> properties;

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof BaseContent)) return false;
      BaseContent that = (BaseContent) o;
      return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(properties);
    }
  }

  public abstract static class BaseContentBuilder<
      SELF extends BaseContentBuilder<SELF, T>, T extends BaseContent> {
    protected Map<String, String> properties;

    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    public T build() {
      T content = internalBuild();
      content.properties = properties;
      return content;
    }

    protected abstract T internalBuild();

    private SELF self() {
      return (SELF) this;
    }
  }

  public abstract static class BasePolicyBuilder<
      SELF extends BasePolicyBuilder<SELF, T>, T extends BasePolicy> {
    protected String name;
    protected String type;
    protected String comment;
    protected boolean enabled;
    protected boolean exclusive;
    protected boolean inheritable;
    protected Set<MetadataObject.Type> supportedObjectTypes;
    protected Content content;
    protected Audit auditInfo;

    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    public SELF withType(String type) {
      this.type = type;
      return self();
    }

    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    public SELF withEnabled(boolean enabled) {
      this.enabled = enabled;
      return self();
    }

    public SELF withExclusive(boolean exclusive) {
      this.exclusive = exclusive;
      return self();
    }

    public SELF withInheritable(boolean inheritable) {
      this.inheritable = inheritable;
      return self();
    }

    public SELF withSupportedObjectTypes(Set<MetadataObject.Type> supportedObjectTypes) {
      this.supportedObjectTypes = supportedObjectTypes;
      return self();
    }

    public SELF withContent(Content content) {
      this.content = content;
      return self();
    }

    public SELF withAuditInfo(Audit auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    public T build() {
      T policy = internalBuild();
      policy.name = name;
      policy.type = type;
      policy.comment = comment;
      policy.enabled = enabled;
      policy.exclusive = exclusive;
      policy.inheritable = inheritable;
      policy.supportedObjectTypes = supportedObjectTypes;
      policy.content = content;
      policy.auditInfo = auditInfo;
      policy.validate();
      return policy;
    }

    protected abstract T internalBuild();

    private SELF self() {
      return (SELF) this;
    }
  }
}
