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
package org.apache.gravitino.dto.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.policy.Policy;

/** Represents a Policy Data Transfer Object (DTO). */
@ToString
public class PolicyDTO implements Policy {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("policyType")
  private String policyType;

  @JsonProperty("enabled")
  private boolean enabled;

  @JsonProperty("exclusive")
  private boolean exclusive;

  @JsonProperty("inheritable")
  private boolean inheritable;

  @JsonProperty("supportedObjectTypes")
  private Set<MetadataObject.Type> supportedObjectTypes = Collections.emptySet();

  @JsonProperty("content")
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
      property = "policyType",
      defaultImpl = PolicyContentDTO.CustomContentDTO.class)
  @JsonSubTypes({
    // add mappings for built-in types here
    // For example: @JsonSubTypes.Type(value = DataCompactionContent.class, name =
    // "system_data_compaction")
  })
  private PolicyContentDTO content;

  @JsonProperty("inherited")
  private Optional<Boolean> inherited = Optional.empty();

  @JsonProperty("audit")
  private AuditDTO audit;

  private PolicyDTO() {}

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PolicyDTO)) return false;
    PolicyDTO policyDTO = (PolicyDTO) o;
    return enabled == policyDTO.enabled
        && exclusive == policyDTO.exclusive
        && inheritable == policyDTO.inheritable
        && Objects.equals(name, policyDTO.name)
        && Objects.equals(comment, policyDTO.comment)
        && Objects.equals(policyType, policyDTO.policyType)
        && Objects.equals(supportedObjectTypes, policyDTO.supportedObjectTypes)
        && Objects.equals(content, policyDTO.content)
        && Objects.equals(audit, policyDTO.audit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        comment,
        policyType,
        enabled,
        exclusive,
        inheritable,
        supportedObjectTypes,
        content,
        audit);
  }

  /**
   * @return a new builder for constructing a PolicyDTO.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String name() {
    return name;
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
  public PolicyContentDTO content() {
    return content;
  }

  @Override
  public Optional<Boolean> inherited() {
    return inherited;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /** Builder class for constructing PolicyDTO instances. */
  public static class Builder {
    private final PolicyDTO policyDTO;

    private Builder() {
      policyDTO = new PolicyDTO();
    }

    /**
     * Sets the name of the policy.
     *
     * @param name The name of the policy.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      policyDTO.name = name;
      return this;
    }

    /**
     * Sets the comment associated with the policy.
     *
     * @param comment The comment associated with the policy.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      policyDTO.comment = comment;
      return this;
    }

    /**
     * Sets the type of the policy.
     *
     * @param policyType The type of the policy.
     * @return The builder instance.
     */
    public Builder withPolicyType(String policyType) {
      policyDTO.policyType = policyType;
      return this;
    }

    /**
     * Sets whether the policy is enabled or not.
     *
     * @param enabled Whether the policy is enabled.
     * @return The builder instance.
     */
    public Builder withEnabled(boolean enabled) {
      policyDTO.enabled = enabled;
      return this;
    }

    /**
     * Sets whether the policy is exclusive or not.
     *
     * @param exclusive Whether the policy is exclusive.
     * @return The builder instance.
     */
    public Builder withExclusive(boolean exclusive) {
      policyDTO.exclusive = exclusive;
      return this;
    }

    /**
     * Sets whether the policy is inheritable or not.
     *
     * @param inheritable Whether the policy is inheritable.
     * @return The builder instance.
     */
    public Builder withInheritable(boolean inheritable) {
      policyDTO.inheritable = inheritable;
      return this;
    }

    /**
     * Sets the set of supported metadata object types for the policy.
     *
     * @param supportedObjectTypes The set of supported metadata object types.
     * @return The builder instance.
     */
    public Builder withSupportedObjectTypes(Set<MetadataObject.Type> supportedObjectTypes) {
      policyDTO.supportedObjectTypes = supportedObjectTypes;
      return this;
    }

    /**
     * Sets the content of the policy.
     *
     * @param content The content of the policy.
     * @return The builder instance.
     */
    public Builder withContent(PolicyContentDTO content) {
      policyDTO.content = content;
      return this;
    }

    /**
     * Sets the audit information for the policy.
     *
     * @param audit The audit information for the policy.
     * @return The builder instance.
     */
    public Builder withAudit(AuditDTO audit) {
      policyDTO.audit = audit;
      return this;
    }

    /**
     * Sets whether the policy is inherited.
     *
     * @param inherited Whether the policy is inherited.
     * @return The builder instance.
     */
    public Builder withInherited(Optional<Boolean> inherited) {
      policyDTO.inherited = inherited;
      return this;
    }

    /**
     * @return The constructed Policy DTO.
     */
    public PolicyDTO build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(policyDTO.name), "policy name cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(policyDTO.policyType), "policy type cannot be empty");
      Preconditions.checkArgument(policyDTO.content != null, "policy content cannot be null");
      Preconditions.checkArgument(
          CollectionUtils.isNotEmpty(policyDTO.supportedObjectTypes),
          "supported objectTypes cannot be empty");
      policyDTO.content.validate();
      return policyDTO;
    }
  }
}
