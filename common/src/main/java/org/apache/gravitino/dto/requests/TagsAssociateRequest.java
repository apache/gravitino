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
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to associate tags. */
@Getter
@EqualsAndHashCode
@ToString
public class TagsAssociateRequest implements RESTRequest {

  @JsonProperty("tagsToAdd")
  private final String[] tagsToAdd;

  @JsonProperty("tagsToRemove")
  private final String[] tagsToRemove;

  /**
   * Creates a new TagsAssociateRequest.
   *
   * @param tagsToAdd The tags to add.
   * @param tagsToRemove The tags to remove.
   */
  public TagsAssociateRequest(String[] tagsToAdd, String[] tagsToRemove) {
    this.tagsToAdd = tagsToAdd;
    this.tagsToRemove = tagsToRemove;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TagsAssociateRequest() {
    this(null, null);
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        tagsToAdd != null || tagsToRemove != null,
        "tagsToAdd and tagsToRemove cannot both be null");

    if (tagsToAdd != null) {
      for (String tag : tagsToAdd) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(tag), "tagsToAdd must not contain null or empty tag names");
      }
    }

    if (tagsToRemove != null) {
      for (String tag : tagsToRemove) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(tag), "tagsToRemove must not contain null or empty tag names");
      }
    }
  }
}
