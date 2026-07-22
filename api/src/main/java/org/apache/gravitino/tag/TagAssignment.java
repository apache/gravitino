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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** Represents a tag assignment in a metadata-object context. */
@Evolving
public final class TagAssignment {

  private static final TagAssignment WITHOUT_VALUES = new TagAssignment(new String[0]);

  private final String[] values;

  private TagAssignment(String[] values) {
    this.values = values.clone();
  }

  /**
   * Creates an assignment without values.
   *
   * @return The tag assignment.
   */
  public static TagAssignment withoutValues() {
    return WITHOUT_VALUES;
  }

  /**
   * Creates an assignment with values.
   *
   * @param values The assignment values.
   * @return The tag assignment.
   */
  public static TagAssignment ofValues(String... values) {
    Preconditions.checkArgument(
        values != null && values.length > 0, "Assignment values must not be null or empty");
    for (String value : values) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(value), "Assignment values must not contain blank values");
    }

    return new TagAssignment(values);
  }

  /**
   * @return True if the assignment has values.
   */
  public boolean hasValues() {
    return values.length > 0;
  }

  /**
   * @return The assignment values.
   */
  public String[] values() {
    return values.clone();
  }

  /**
   * Compares this assignment with another object.
   *
   * @param o The object to compare.
   * @return True if the object is equal to this assignment.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagAssignment)) {
      return false;
    }

    TagAssignment that = (TagAssignment) o;
    return Arrays.equals(values, that.values);
  }

  /**
   * @return The hash code of this assignment.
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  /**
   * @return The string representation of this assignment.
   */
  @Override
  public String toString() {
    return "TagAssignment{" + "values=" + Arrays.toString(values) + "}";
  }
}
