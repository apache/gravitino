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

package org.apache.gravitino.tag;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** Describes what assignment values a tag accepts. */
@Evolving
public final class TagValueConstraint {

  private static final TagValueConstraint ANY_VALUE =
      new TagValueConstraint(Type.ANY_VALUE, new String[0]);

  private static final TagValueConstraint WITHOUT_VALUES =
      new TagValueConstraint(Type.WITHOUT_VALUES, new String[0]);

  private final Type type;
  private final String[] allowedValues;

  private TagValueConstraint(Type type, String[] allowedValues) {
    this.type = type;
    this.allowedValues = allowedValues.clone();
  }

  /** The value constraint type. */
  public enum Type {
    /** The tag accepts any non-empty assignment value. */
    ANY_VALUE,

    /** The tag only accepts assignments without values. */
    WITHOUT_VALUES,

    /** The tag only accepts values from the allowed value list. */
    ALLOWED_VALUES
  }

  /**
   * Creates a constraint that accepts any non-empty assignment value.
   *
   * @return The value constraint.
   */
  public static TagValueConstraint anyValue() {
    return ANY_VALUE;
  }

  /**
   * Creates a constraint that only accepts assignments without values.
   *
   * @return The value constraint.
   */
  public static TagValueConstraint withoutValues() {
    return WITHOUT_VALUES;
  }

  /**
   * Creates a constraint that only accepts values from the allowed value list.
   *
   * @param allowedValues The allowed assignment values.
   * @return The value constraint.
   */
  public static TagValueConstraint ofAllowedValues(String... allowedValues) {
    Preconditions.checkArgument(
        allowedValues != null && allowedValues.length > 0,
        "Allowed values must not be null or empty");
    for (String allowedValue : allowedValues) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(allowedValue), "Allowed values must not contain blank values");
    }

    return new TagValueConstraint(Type.ALLOWED_VALUES, allowedValues);
  }

  /**
   * @return The value constraint type.
   */
  public Type type() {
    return type;
  }

  /**
   * @return The allowed values. Empty when the type is not {@link Type#ALLOWED_VALUES}.
   */
  public String[] allowedValues() {
    return allowedValues.clone();
  }

  /**
   * Compares this constraint with another object.
   *
   * @param o The object to compare.
   * @return True if the object is equal to this constraint.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagValueConstraint)) {
      return false;
    }

    TagValueConstraint that = (TagValueConstraint) o;
    return type == that.type && Arrays.equals(allowedValues, that.allowedValues);
  }

  /**
   * @return The hash code of this constraint.
   */
  @Override
  public int hashCode() {
    int result = Objects.hash(type);
    result = 31 * result + Arrays.hashCode(allowedValues);
    return result;
  }

  /**
   * @return The string representation of this constraint.
   */
  @Override
  public String toString() {
    return "TagValueConstraint{"
        + "type="
        + type
        + ", allowedValues="
        + Arrays.toString(allowedValues)
        + "}";
  }
}
