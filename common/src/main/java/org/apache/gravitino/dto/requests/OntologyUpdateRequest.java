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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.ontology.OntologyDefinitionDTO;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents one update to apply to an ontology. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = OntologyUpdateRequest.RenameOntologyRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = OntologyUpdateRequest.UpdateOntologyCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = OntologyUpdateRequest.SetOntologyPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = OntologyUpdateRequest.RemoveOntologyPropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = OntologyUpdateRequest.ReplaceOntologyDefinitionRequest.class,
      name = "replaceDefinition"),
  @JsonSubTypes.Type(
      value = OntologyUpdateRequest.UpdateOntologyStatusRequest.class,
      name = "updateStatus")
})
public interface OntologyUpdateRequest extends RESTRequest {

  /**
   * Returns the ontology change this request represents.
   *
   * @return The ontology change.
   */
  OntologyChange ontologyChange();

  /** The request to rename an ontology. */
  @EqualsAndHashCode
  @ToString
  class RenameOntologyRequest implements OntologyUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Creates a new rename request.
     *
     * @param newName The new ontology name.
     */
    public RenameOntologyRequest(String newName) {
      this.newName = newName;
    }

    /** This is the constructor that is used by Jackson deserializer. */
    public RenameOntologyRequest() {
      this.newName = null;
    }

    @Override
    public OntologyChange ontologyChange() {
      return OntologyChange.rename(newName);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(newName), "\"newName\" must not be blank");
    }
  }

  /** The request to update the comment of an ontology. */
  @EqualsAndHashCode
  @ToString
  class UpdateOntologyCommentRequest implements OntologyUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    @Nullable
    private final String newComment;

    /**
     * Creates a new comment update request.
     *
     * @param newComment The new comment, or {@code null} to clear it.
     */
    public UpdateOntologyCommentRequest(@Nullable String newComment) {
      this.newComment = newComment;
    }

    /** This is the constructor that is used by Jackson deserializer. */
    public UpdateOntologyCommentRequest() {
      this.newComment = null;
    }

    @Override
    public OntologyChange ontologyChange() {
      return OntologyChange.updateComment(newComment);
    }

    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** The request to set one property of an ontology. */
  @EqualsAndHashCode
  @ToString
  class SetOntologyPropertyRequest implements OntologyUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new set property request.
     *
     * @param property The property name.
     * @param value The property value.
     */
    public SetOntologyPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** This is the constructor that is used by Jackson deserializer. */
    public SetOntologyPropertyRequest() {
      this.property = null;
      this.value = null;
    }

    @Override
    public OntologyChange ontologyChange() {
      return OntologyChange.setProperty(property, value);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" must not be blank");
    }
  }

  /** The request to remove one property of an ontology. */
  @EqualsAndHashCode
  @ToString
  class RemoveOntologyPropertyRequest implements OntologyUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Creates a new remove property request.
     *
     * @param property The property name.
     */
    public RemoveOntologyPropertyRequest(String property) {
      this.property = property;
    }

    /** This is the constructor that is used by Jackson deserializer. */
    public RemoveOntologyPropertyRequest() {
      this.property = null;
    }

    @Override
    public OntologyChange ontologyChange() {
      return OntologyChange.removeProperty(property);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" must not be blank");
    }
  }

  /** The request to replace the definition of an ontology. */
  @EqualsAndHashCode
  @ToString
  class ReplaceOntologyDefinitionRequest implements OntologyUpdateRequest {

    @Getter
    @JsonProperty("definition")
    private final OntologyDefinitionDTO definition;

    /**
     * Creates a new replace definition request.
     *
     * @param definition The new definition.
     */
    public ReplaceOntologyDefinitionRequest(OntologyDefinitionDTO definition) {
      this.definition = definition;
    }

    /** This is the constructor that is used by Jackson deserializer. */
    public ReplaceOntologyDefinitionRequest() {
      this.definition = null;
    }

    @Override
    public OntologyChange ontologyChange() {
      return OntologyChange.replaceDefinition(definition.toOntologyDefinition());
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(definition != null, "\"definition\" must not be null");
      definition.validate();
    }
  }

  /** The request to change the lifecycle status of an ontology. */
  @EqualsAndHashCode
  @ToString
  class UpdateOntologyStatusRequest implements OntologyUpdateRequest {

    @Getter
    @JsonProperty("status")
    private final Ontology.Status status;

    /**
     * Creates a new status update request.
     *
     * @param status The new status.
     */
    public UpdateOntologyStatusRequest(Ontology.Status status) {
      this.status = status;
    }

    /** This is the constructor that is used by Jackson deserializer. */
    public UpdateOntologyStatusRequest() {
      this.status = null;
    }

    @Override
    public OntologyChange ontologyChange() {
      return OntologyChange.updateStatus(status);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(status != null, "\"status\" must not be null");
    }
  }
}
