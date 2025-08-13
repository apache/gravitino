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

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a policy. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = PolicyUpdateRequest.RenamePolicyRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = PolicyUpdateRequest.UpdatePolicyCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = PolicyUpdateRequest.UpdatePolicyContentRequest.class,
      name = "updateContent")
})
public interface PolicyUpdateRequest extends RESTRequest {

  /**
   * Returns the policy change.
   *
   * @return the policy change.
   */
  PolicyChange policyChange();

  /** The policy update request for renaming a policy. */
  @EqualsAndHashCode
  @Getter
  @ToString
  class RenamePolicyRequest implements PolicyUpdateRequest {

    @JsonProperty("newName")
    private final String newName;

    /**
     * Creates a new RenamePolicyRequest.
     *
     * @param newName The new name of the policy.
     */
    public RenamePolicyRequest(String newName) {
      this.newName = newName;
    }

    /** This is the constructor that is used by Jackson deserializer */
    private RenamePolicyRequest() {
      this(null);
    }

    @Override
    public PolicyChange policyChange() {
      return PolicyChange.rename(newName);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(newName), "\"newName\" must not be blank");
    }
  }

  /** The policy update request for updating a policy comment. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class UpdatePolicyCommentRequest implements PolicyUpdateRequest {

    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Creates a new UpdatePolicyCommentRequest.
     *
     * @param newComment The new comment of the policy.
     */
    public UpdatePolicyCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** This is the constructor that is used by Jackson deserializer */
    private UpdatePolicyCommentRequest() {
      this(null);
    }

    @Override
    public PolicyChange policyChange() {
      return PolicyChange.updateComment(newComment);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** The policy update request for updating a policy content. */
  @EqualsAndHashCode
  @Getter
  @ToString
  class UpdatePolicyContentRequest implements PolicyUpdateRequest {

    @JsonProperty("policyType")
    private final String policyType;

    @JsonProperty("newContent")
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
    private final PolicyContentDTO newContent;

    /**
     * Creates a new UpdatePolicyContentRequest.
     *
     * @param policyType The type of the policy, used for validation and serialization. This should
     *     match the type of the content being updated.
     * @param newContent The new content of the policy.
     */
    public UpdatePolicyContentRequest(String policyType, PolicyContentDTO newContent) {
      this.policyType = policyType;
      this.newContent = newContent;
    }

    /** This is the constructor that is used by Jackson deserializer */
    private UpdatePolicyContentRequest() {
      this(null, null);
    }

    @Override
    public PolicyChange policyChange() {
      return PolicyChange.updateContent(policyType, fromDTO(newContent));
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(newContent != null, "\"newContent\" must not be null");
      newContent.validate();
    }
  }
}
