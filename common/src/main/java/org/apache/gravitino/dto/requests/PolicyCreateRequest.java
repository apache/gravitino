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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
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
  })
  private final PolicyContentDTO policyContent;

  /**
   * Creates a new PolicyCreateRequest.
   *
   * @param name The name of the policy.
   * @param comment The comment of the policy.
   * @param type The type of the policy.
   * @param enabled Whether the policy is enabled.
   * @param content The content of the policy.
   */
  public PolicyCreateRequest(
      String name, String type, String comment, boolean enabled, PolicyContentDTO content) {
    this.name = name;
    this.policyType = type;
    this.comment = comment;
    this.enabled = enabled;
    this.policyContent = content;
  }

  /** This is the constructor that is used by Jackson deserializer */
  private PolicyCreateRequest() {
    this(null, null, null, true, null);
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
    Preconditions.checkArgument(
        policyContent != null, "\"content\" is required and cannot be null");
    policyContent.validate();
  }
}
