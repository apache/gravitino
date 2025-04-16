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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.rest.RESTRequest;

/** Request to update a model version. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.UpdateModelVersionComment.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.RemoveModelVersionComment.class,
      name = "removeComment"),
})
public interface ModelVersionUpdateRequest extends RESTRequest {

  /**
   * Returns the model version change.
   *
   * @return the model version change.
   */
  ModelVersionChange modelVersionChange();

  /** Request to update comment of a model version */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class UpdateModelVersionComment implements ModelVersionUpdateRequest {
    @JsonProperty("newComment")
    private final String newComment;

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.updateComment(newComment);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** Request to remove comment from a model version */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @ToString
  class RemoveModelVersionComment implements ModelVersionUpdateRequest {

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.removeComment();
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }
}
