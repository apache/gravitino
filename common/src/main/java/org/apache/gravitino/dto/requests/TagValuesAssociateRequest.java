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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.tag.TagValue;

/** Represents a request to associate tags with optional values. */
public class TagValuesAssociateRequest implements RESTRequest {

  private static final int MAX_TAG_VALUE_LENGTH = 256;

  @JsonProperty("tagsToAdd")
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  private final RequestTagValue[] tagsToAdd;

  @JsonProperty("tagsToRemove")
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  private final RequestTagValue[] tagsToRemove;

  /**
   * Creates a new TagValuesAssociateRequest.
   *
   * @param tagsToAdd The tag values to add.
   * @param tagsToRemove The tag values to remove.
   */
  public TagValuesAssociateRequest(TagValue[] tagsToAdd, TagValue[] tagsToRemove) {
    this.tagsToAdd = toRequestTagValues(tagsToAdd);
    this.tagsToRemove = toRequestTagValues(tagsToRemove);
  }

  /** This is the constructor that is used by Jackson deserializer */
  public TagValuesAssociateRequest() {
    this(null, null);
  }

  /**
   * Returns the tag values to add.
   *
   * @return The tag values to add.
   */
  public TagValue[] tagValuesToAdd() {
    return toTagValues(tagsToAdd);
  }

  /**
   * Returns the tag values to remove.
   *
   * @return The tag values to remove.
   */
  public TagValue[] tagValuesToRemove() {
    return toTagValues(tagsToRemove);
  }

  /**
   * Returns the tag names to add without validating assignment values.
   *
   * @return The tag names to add.
   */
  public String[] tagNamesToAdd() {
    return tagNames(tagsToAdd);
  }

  /**
   * Returns the tag names to remove without validating assignment values.
   *
   * @return The tag names to remove.
   */
  public String[] tagNamesToRemove() {
    return tagNames(tagsToRemove);
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        tagsToAdd.length > 0 || tagsToRemove.length > 0,
        "tagsToAdd and tagsToRemove cannot both be empty");

    validateTagValues(tagsToAdd, "tagsToAdd");
    validateTagValues(tagsToRemove, "tagsToRemove");
    validateNoIntersection(tagsToAdd, tagsToRemove);
  }

  private static RequestTagValue[] toRequestTagValues(TagValue[] tagValues) {
    if (tagValues == null) {
      return new RequestTagValue[0];
    }
    return Arrays.stream(tagValues).map(RequestTagValue::new).toArray(RequestTagValue[]::new);
  }

  private static TagValue[] toTagValues(RequestTagValue[] tagValues) {
    if (tagValues == null) {
      return new TagValue[0];
    }
    return Arrays.stream(tagValues).map(RequestTagValue::toTagValue).toArray(TagValue[]::new);
  }

  private static String[] tagNames(RequestTagValue[] tagValues) {
    if (tagValues == null) {
      return new String[0];
    }
    return Arrays.stream(tagValues)
        .filter(tagValue -> tagValue != null)
        .map(tagValue -> tagValue.name)
        .toArray(String[]::new);
  }

  private static void validateTagValues(RequestTagValue[] tagValues, String fieldName) {
    for (RequestTagValue tagValue : tagValues) {
      Preconditions.checkArgument(
          tagValue != null, "%s must not contain null tag values", fieldName);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(tagValue.name),
          "%s must not contain null or empty tag names",
          fieldName);
      if (tagValue.value != null) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(tagValue.value),
            "%s must not contain null or empty tag values",
            fieldName);
        Preconditions.checkArgument(
            tagValue.value.length() <= MAX_TAG_VALUE_LENGTH,
            "%s tag values must not exceed %s characters",
            fieldName,
            MAX_TAG_VALUE_LENGTH);
      }
    }
  }

  private static void validateNoIntersection(
      RequestTagValue[] tagsToAdd, RequestTagValue[] tagsToRemove) {
    Set<RequestTagValue> tagsToAddSet = new LinkedHashSet<>(Arrays.asList(tagsToAdd));
    for (RequestTagValue tagToRemove : tagsToRemove) {
      Preconditions.checkArgument(
          !tagsToAddSet.contains(tagToRemove), "tagsToAdd and tagsToRemove must not overlap");
    }
  }

  /**
   * Compares this request with another object.
   *
   * @param o The object to compare.
   * @return True if the object is equal to this request.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagValuesAssociateRequest)) {
      return false;
    }

    TagValuesAssociateRequest that = (TagValuesAssociateRequest) o;
    return Arrays.equals(tagsToAdd, that.tagsToAdd)
        && Arrays.equals(tagsToRemove, that.tagsToRemove);
  }

  /**
   * @return The hash code of this request.
   */
  @Override
  public int hashCode() {
    int result = Arrays.hashCode(tagsToAdd);
    result = 31 * result + Arrays.hashCode(tagsToRemove);
    return result;
  }

  /**
   * @return The string representation of this request.
   */
  @Override
  public String toString() {
    return "TagValuesAssociateRequest{"
        + "tagsToAdd="
        + Arrays.toString(tagsToAdd)
        + ", tagsToRemove="
        + Arrays.toString(tagsToRemove)
        + "}";
  }

  @EqualsAndHashCode
  @ToString
  static class RequestTagValue {
    @JsonProperty("name")
    private String name;

    @JsonProperty("value")
    @Nullable
    private String value;

    private RequestTagValue() {}

    private RequestTagValue(TagValue tagValue) {
      this.name = tagValue.name();
      this.value = tagValue.value().orElse(null);
    }

    private TagValue toTagValue() {
      return value == null ? TagValue.noValue(name) : TagValue.of(name, value);
    }
  }
}
