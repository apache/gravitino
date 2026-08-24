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
package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.tag.TagValueConstraint;

/**
 * Provides access to metadata about a Tag instance, designed for use by event listeners. This class
 * encapsulates the essential attributes of a Tag, including its name, optional description,
 * properties, and audit information.
 */
@DeveloperApi
public final class TagInfo {
  private final String name;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final String[] allowedValues;
  @Nullable private final String[] assignmentValues;

  /**
   * Directly constructs TagInfo with specified details.
   *
   * @param name The name of the Tag.
   * @param comment An optional description for the Tag.
   * @param properties A map of properties associated with the Tag.
   */
  public TagInfo(String name, String comment, Map<String, String> properties) {
    this(name, comment, properties, null, null);
  }

  /**
   * Directly constructs TagInfo with specified details.
   *
   * @param name The name of the Tag.
   * @param comment An optional description for the Tag.
   * @param properties A map of properties associated with the Tag.
   * @param allowedValues The optional allowed assignment values of the Tag.
   * @param assignmentValues The optional assignment values of the Tag.
   */
  public TagInfo(
      String name,
      String comment,
      Map<String, String> properties,
      @Nullable String[] allowedValues,
      @Nullable String[] assignmentValues) {
    this.name = name;
    this.comment = comment;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.allowedValues =
        allowedValues == null ? null : Arrays.copyOf(allowedValues, allowedValues.length);
    this.assignmentValues =
        assignmentValues == null ? null : Arrays.copyOf(assignmentValues, assignmentValues.length);
  }

  /**
   * Returns the name of the Tag.
   *
   * @return The Tag's name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the optional comment describing the Tag.
   *
   * @return The comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the properties of the Tag.
   *
   * @return A map of Tag properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the assignment value constraint of the Tag.
   *
   * @return The assignment value constraint.
   */
  public TagValueConstraint valueConstraint() {
    if (allowedValues == null) {
      return TagValueConstraint.anyValue();
    }
    if (allowedValues.length == 0) {
      return TagValueConstraint.noValue();
    }
    return TagValueConstraint.ofAllowedValues(allowedValues);
  }

  /**
   * Returns the assignment values of the Tag in metadata-object tag events.
   *
   * @return The assignment in context, or empty when no assignment context exists.
   */
  public Optional<TagAssignment> assignment() {
    if (assignmentValues == null) {
      return Optional.empty();
    }
    if (assignmentValues.length == 0) {
      return Optional.of(TagAssignment.noValue());
    }
    return Optional.of(TagAssignment.ofValues(assignmentValues));
  }
}
