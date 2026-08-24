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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.tag.TagAssignment;

/** A selector that restricts when a policy-to-tag association matches a tag assignment. */
@Evolving
public final class PolicyTagSelector {

  /** Supported policy-to-tag selector types. */
  public enum Type {
    /** Matches when the effective tag assignment contains one exact value. */
    TAG_VALUE
  }

  private final Type type;
  private final String value;

  private PolicyTagSelector(Type type, String value) {
    this.type = type;
    this.value = value;
  }

  /**
   * Creates a selector that matches one exact tag assignment value.
   *
   * @param value The tag assignment value to match.
   * @return The selector.
   */
  public static PolicyTagSelector tagValue(String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "Selector value cannot be blank");
    return new PolicyTagSelector(Type.TAG_VALUE, value);
  }

  /**
   * @return The selector type.
   */
  public Type type() {
    return type;
  }

  /**
   * @return The exact tag assignment value matched by this selector.
   */
  public String value() {
    return value;
  }

  /**
   * Tests whether this selector matches a tag assignment.
   *
   * @param assignment The effective tag assignment.
   * @return True when the selector matches, false otherwise.
   */
  public boolean matches(TagAssignment assignment) {
    Preconditions.checkArgument(assignment != null, "Tag assignment cannot be null");
    switch (type) {
      case TAG_VALUE:
        return Arrays.asList(assignment.values()).contains(value);
      default:
        throw new IllegalArgumentException("Unsupported selector type: " + type);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolicyTagSelector)) {
      return false;
    }
    PolicyTagSelector that = (PolicyTagSelector) o;
    return type == that.type && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }

  @Override
  public String toString() {
    return "PolicyTagSelector{" + "type=" + type + ", value='" + value + '\'' + '}';
  }
}
