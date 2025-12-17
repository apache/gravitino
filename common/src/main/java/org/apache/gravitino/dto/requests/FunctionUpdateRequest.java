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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionSignatureDTO;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.rest.RESTRequest;

/** Request to update a function. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = FunctionUpdateRequest.UpdateCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = FunctionUpdateRequest.UpdateImplementationsRequest.class,
      name = "updateImpls"),
  @JsonSubTypes.Type(value = FunctionUpdateRequest.AddImplementationRequest.class, name = "addImpl")
})
public interface FunctionUpdateRequest extends RESTRequest {

  /**
   * Converts this request to a {@link FunctionChange}.
   *
   * @return The function change represented by the request.
   */
  FunctionChange functionChange();

  @Getter
  @EqualsAndHashCode
  @ToString
  class UpdateCommentRequest implements FunctionUpdateRequest {

    @JsonProperty("signature")
    private FunctionSignatureDTO signature;

    @JsonProperty("newComment")
    private String newComment;

    private UpdateCommentRequest() {
      this(null, null);
    }

    /**
     * Creates a comment update request.
     *
     * @param signature Optional function signature to target.
     * @param newComment New comment value.
     */
    public UpdateCommentRequest(FunctionSignatureDTO signature, String newComment) {
      this.signature = signature;
      this.newComment = newComment;
    }

    /** {@inheritDoc} */
    @Override
    public FunctionChange functionChange() {
      if (signature == null) {
        return FunctionChange.updateComment(newComment);
      }
      return FunctionChange.updateComment(signature.toFunctionSignature(), newComment);
    }

    /** {@inheritDoc} */
    @Override
    public void validate() throws IllegalArgumentException {
      if (signature != null) {
        signature.validate();
      }
    }
  }

  @Getter
  @EqualsAndHashCode
  @ToString
  @AllArgsConstructor
  @NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
  class UpdateImplementationsRequest implements FunctionUpdateRequest {

    @JsonProperty("signature")
    private FunctionSignatureDTO signature;

    @JsonProperty("newImpls")
    private FunctionImplDTO[] newImpls;

    /** {@inheritDoc} */
    @Override
    public FunctionChange functionChange() {
      FunctionChange change =
          FunctionChange.updateImplementations(
              signature == null ? null : signature.toFunctionSignature(),
              Arrays.stream(newImpls)
                  .map(FunctionImplDTO::toFunctionImpl)
                  .toArray(org.apache.gravitino.function.FunctionImpl[]::new));
      return change;
    }

    /** {@inheritDoc} */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          newImpls != null && newImpls.length > 0, "\"newImpls\" is required");
      if (signature != null) {
        signature.validate();
      }
      Arrays.stream(newImpls).forEach(FunctionImplDTO::validate);
    }
  }

  @Getter
  @EqualsAndHashCode
  @ToString
  @AllArgsConstructor
  @NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
  class AddImplementationRequest implements FunctionUpdateRequest {

    @JsonProperty("signature")
    private FunctionSignatureDTO signature;

    @JsonProperty("implementation")
    private FunctionImplDTO implementation;

    /** {@inheritDoc} */
    @Override
    public FunctionChange functionChange() {
      return FunctionChange.addImplementation(
          signature == null ? null : signature.toFunctionSignature(),
          implementation.toFunctionImpl());
    }

    /** {@inheritDoc} */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          implementation != null, "\"implementation\" field is required and cannot be null");
      if (signature != null) {
        signature.validate();
      }
      implementation.validate();
    }
  }
}
