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
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a domain. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = DomainUpdateRequest.RenameDomainRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = DomainUpdateRequest.UpdateDomainCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = DomainUpdateRequest.SetDomainPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = DomainUpdateRequest.RemoveDomainPropertyRequest.class,
      name = "removeProperty")
})
public interface DomainUpdateRequest extends RESTRequest {

  /**
   * Returns the domain change.
   *
   * @return the domain change.
   */
  DomainChange domainChange();

  /** The domain update request for renaming a domain. */
  @EqualsAndHashCode
  @ToString
  class RenameDomainRequest implements DomainUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Creates a new RenameDomainRequest.
     *
     * @param newName The new name of the domain.
     */
    public RenameDomainRequest(String newName) {
      this.newName = newName;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public RenameDomainRequest() {
      this.newName = null;
    }

    @Override
    public DomainChange domainChange() {
      return DomainChange.rename(newName);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(StringUtils.isNotBlank(newName), "\"newName\" must not be blank");
    }
  }

  /** The domain update request for updating a domain comment. */
  @EqualsAndHashCode
  @ToString
  class UpdateDomainCommentRequest implements DomainUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    @Nullable
    private final String newComment;

    /**
     * Creates a new UpdateDomainCommentRequest.
     *
     * @param newComment The new comment of the domain.
     */
    public UpdateDomainCommentRequest(@Nullable String newComment) {
      this.newComment = newComment;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public UpdateDomainCommentRequest() {
      this.newComment = null;
    }

    @Override
    public DomainChange domainChange() {
      return DomainChange.updateComment(newComment);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }

  /** The domain update request for setting a domain property. */
  @EqualsAndHashCode
  @ToString
  class SetDomainPropertyRequest implements DomainUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new SetDomainPropertyRequest.
     *
     * @param property The property to set.
     * @param value The value of the property.
     */
    public SetDomainPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public SetDomainPropertyRequest() {
      this.property = null;
      this.value = null;
    }

    @Override
    public DomainChange domainChange() {
      return DomainChange.setProperty(property, value);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" must not be blank");
      Preconditions.checkArgument(value != null, "\"value\" must not be null");
    }
  }

  /** The domain update request for removing a domain property. */
  @EqualsAndHashCode
  @ToString
  class RemoveDomainPropertyRequest implements DomainUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Creates a new RemoveDomainPropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveDomainPropertyRequest(String property) {
      this.property = property;
    }

    /** This is the constructor that is used by Jackson deserializer */
    public RemoveDomainPropertyRequest() {
      this.property = null;
    }

    @Override
    public DomainChange domainChange() {
      return DomainChange.removeProperty(property);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" must not be blank");
    }
  }
}
