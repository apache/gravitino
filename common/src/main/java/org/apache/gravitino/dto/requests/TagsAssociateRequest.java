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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.tag.TagValuePair;

/** Represents a request to associate tags. */
@Getter
@EqualsAndHashCode
@ToString
public class TagsAssociateRequest implements RESTRequest {

  private static final int MAX_TAG_VALUE_LENGTH = 256;

  @JsonProperty("tagsToAdd")
  private final TagValuePair[] tagsToAdd;

  @JsonProperty("tagsToRemove")
  private final TagValuePair[] tagsToRemove;

  /**
   * Creates a new TagsAssociateRequest.
   *
   * @param tagsToAdd The tag-value pairs to add.
   * @param tagsToRemove The tag-value pairs to remove.
   */
  public TagsAssociateRequest(TagValuePair[] tagsToAdd, TagValuePair[] tagsToRemove) {
    this.tagsToAdd = tagsToAdd;
    this.tagsToRemove = tagsToRemove;
  }

  /**
   * Creates a new TagsAssociateRequest with valueless tag names.
   *
   * @param tagsToAdd The tags to add.
   * @param tagsToRemove The tags to remove.
   */
  public TagsAssociateRequest(String[] tagsToAdd, String[] tagsToRemove) {
    this(toValuelessPairs(tagsToAdd), toValuelessPairs(tagsToRemove));
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TagsAssociateRequest() {
    this((TagValuePair[]) null, (TagValuePair[]) null);
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

    validatePairs(tagsToAdd, "tagsToAdd");
    validatePairs(tagsToRemove, "tagsToRemove");
  }

  private static TagValuePair[] toValuelessPairs(String[] tags) {
    if (tags == null) {
      return null;
    }
    return Arrays.stream(tags).map(TagValuePair::valueless).toArray(TagValuePair[]::new);
  }

  private static void validatePairs(TagValuePair[] pairs, String fieldName) {
    if (pairs == null) {
      return;
    }

    for (TagValuePair pair : pairs) {
      Preconditions.checkArgument(pair != null, "%s must not contain null pairs", fieldName);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(pair.name()),
          "%s must not contain null or empty tag names",
          fieldName);
      if (pair.value().isPresent()) {
        String value = pair.value().get();
        Preconditions.checkArgument(
            StringUtils.isNotBlank(value),
            "%s must not contain null or empty tag values",
            fieldName);
        Preconditions.checkArgument(
            value.length() <= MAX_TAG_VALUE_LENGTH,
            "%s tag values must not exceed %s characters",
            fieldName,
            MAX_TAG_VALUE_LENGTH);
      }
    }
  }
}
