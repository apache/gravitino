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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a policy. */
@Getter
@EqualsAndHashCode
@ToString
public class PolicyCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("comment")
  @Nullable
  private final String comment;

  @JsonProperty("policyType")
  private final String policyType;

  @JsonProperty(value = "enabled", defaultValue = "true")
  private final Boolean enabled;

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
    @JsonSubTypes.Type(
        value = PolicyContentDTO.DataCompactionContentDTO.class,
        name = "system_data_compaction")
  })
  private final PolicyContentDTO policyContent;

  @JsonProperty("exclusive")
  private final Boolean exclusive;

  @JsonProperty("inheritable")
  private final Boolean inheritable;

  @JsonProperty("supportedObjectTypes")
  private final Set<MetadataObject.Type> supportedObjectTypes;

  /**
   * Creates a new PolicyCreateRequest.
   *
   * @param name The name of the policy.
   * @param comment The comment of the policy.
   * @param type The type of the policy.
   * @param enabled Whether the policy is enabled.
   * @param exclusive Whether the policy is exclusive.
   * @param inheritable Whether the policy is inheritable.
   * @param supportedObjectTypes The set of metadata object types that the policy can be applied to.
   * @param content The content of the policy.
   */
  public PolicyCreateRequest(
      String name,
      String type,
      String comment,
      boolean enabled,
      boolean exclusive,
      boolean inheritable,
      Set<MetadataObject.Type> supportedObjectTypes,
      PolicyContentDTO content) {
    this.name = name;
    this.policyType = type;
    this.comment = comment;
    this.enabled = enabled;
    this.exclusive = exclusive;
    this.inheritable = inheritable;
    this.supportedObjectTypes = supportedObjectTypes;
    this.policyContent = content;
  }

  /** This is the constructor that is used by Jackson deserializer */
  private PolicyCreateRequest() {
    this(null, null, null, true, false, false, null, null);
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(policyType), "\"policyType\" is required and cannot be empty");

    Policy.BuiltInType builtInType = Policy.BuiltInType.fromPolicyType(policyType);
    if (builtInType != Policy.BuiltInType.CUSTOM) {
      if (exclusive != null) {
        Preconditions.checkArgument(
            exclusive.equals(builtInType.exclusive()),
            String.format(
                "Exclusive flag for built-in policy type '%s' must be %s",
                builtInType, builtInType.exclusive()));
      }

      if (inheritable != null) {
        Preconditions.checkArgument(
            inheritable.equals(builtInType.inheritable()),
            String.format(
                "Inheritable flag for built-in policy type '%s' must be %s",
                builtInType, builtInType.inheritable()));
      }

      if (supportedObjectTypes != null) {
        Preconditions.checkArgument(
            builtInType.supportedObjectTypes().equals(supportedObjectTypes),
            String.format(
                "Supported object types for built-in policy type '%s' must be %s",
                builtInType, builtInType.supportedObjectTypes()));
      }
    } else {
      Preconditions.checkArgument(
          exclusive != null, "\"exclusive\" is required for custom policy type");
      Preconditions.checkArgument(
          inheritable != null, "\"inheritable\" is required for custom policy type");
      Preconditions.checkArgument(
          supportedObjectTypes != null,
          "\"supportedObjectTypes\" is required for custom policy type");
    }
    Preconditions.checkArgument(
        policyContent != null, "\"content\" is required and cannot be null");
    policyContent.validate();
  }
}
