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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.rest.RESTRequest;

/** Request to update a model version. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.UpdateModelVersionComment.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.SetModelVersionPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.RemoveModelVersionPropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.UpdateModelVersionUriRequest.class,
      name = "updateUri"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.AddModelVersionUriRequest.class,
      name = "addUri"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.RemoveModelVersionUriRequest.class,
      name = "removeUri"),
  @JsonSubTypes.Type(
      value = ModelVersionUpdateRequest.UpdateModelVersionAliasesRequest.class,
      name = "updateAliases")
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

  /** Request to set Properties of a model version */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class SetModelVersionPropertyRequest implements ModelVersionUpdateRequest {
    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.setProperty(property, value);
    }

    /**
     * Validates the request, i.e., checks if the property and value are not empty and not null.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }
  }

  /** Request to remove a property from a model version. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class RemoveModelVersionPropertyRequest implements ModelVersionUpdateRequest {
    @JsonProperty("property")
    private final String property;

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.removeProperty(property);
    }

    /**
     * Validates the request, i.e., checks if the property is not empty.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }
  }

  /** Request to update the URI of a model version. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class UpdateModelVersionUriRequest implements ModelVersionUpdateRequest {
    @JsonProperty("uriName")
    private final String uriName;

    @JsonProperty("newUri")
    private final String newUri;

    /**
     * Constructor for UpdateModelVersionUriRequest.
     *
     * @param newUri the new uri of the model version.
     */
    public UpdateModelVersionUriRequest(String newUri) {
      this(null, newUri);
    }

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      if (StringUtils.isNotBlank(uriName)) {
        return ModelVersionChange.updateUri(uriName, newUri);
      }
      return ModelVersionChange.updateUri(newUri);
    }

    /**
     * Validates the request, i.e., checks if the newUri is not empty.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newUri), "\"newUri\" field is required and cannot be empty");
    }
  }

  /** Request to add the URI of a model version. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class AddModelVersionUriRequest implements ModelVersionUpdateRequest {
    @JsonProperty("uriName")
    private final String uriName;

    @JsonProperty("uri")
    private final String uri;

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.addUri(uriName, uri);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uriName), "\"uriName\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri), "\"uri\" field is required and cannot be empty");
    }
  }

  /** Request to remove the URI of a model version. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class RemoveModelVersionUriRequest implements ModelVersionUpdateRequest {
    @JsonProperty("uriName")
    private final String uriName;

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.removeUri(uriName);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uriName), "\"uriName\" field is required and cannot be empty");
    }
  }

  /** Request to update the aliases of a model version. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class UpdateModelVersionAliasesRequest implements ModelVersionUpdateRequest {
    @JsonProperty("aliasesToAdd")
    private final String[] aliasesToAdd;

    @JsonProperty("aliasesToRemove")
    private final String[] aliasesToRemove;

    /** {@inheritDoc} */
    @Override
    public ModelVersionChange modelVersionChange() {
      return ModelVersionChange.updateAliases(aliasesToAdd, aliasesToRemove);
    }

    /**
     * Validates the request
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          aliasesToAdd != null || aliasesToRemove != null,
          "At least one of aliasesToAdd or aliasesToRemove must be non-null");
    }
  }
}
