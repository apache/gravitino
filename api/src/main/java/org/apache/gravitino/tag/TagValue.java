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
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** Represents one tag assignment value in tag association requests. */
@Evolving
public final class TagValue {

  private final String name;

  @Nullable private final String value;

  private TagValue(String name, @Nullable String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Creates a valued tag assignment value.
   *
   * @param name The tag name.
   * @param value The assignment value.
   * @return The tag value.
   */
  public static TagValue of(String name, String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "Tag name must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "Tag value must not be blank");
    return new TagValue(name, value);
  }

  /**
   * Creates a valueless tag assignment value.
   *
   * @param name The tag name.
   * @return The tag value.
   */
  public static TagValue valueless(String name) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "Tag name must not be blank");
    return new TagValue(name, null);
  }

  /**
   * @return The tag name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The optional assignment value.
   */
  public Optional<String> value() {
    return Optional.ofNullable(value);
  }

  /**
   * Compares this value with another object.
   *
   * @param o The object to compare.
   * @return True if the object is equal to this value.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagValue)) {
      return false;
    }

    TagValue that = (TagValue) o;
    return Objects.equals(name, that.name) && Objects.equals(value, that.value);
  }

  /**
   * @return The hash code of this tag value.
   */
  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  /**
   * @return The string representation of this tag value.
   */
  @Override
  public String toString() {
    return "TagValue{" + "name=" + name + ", value=" + value + "}";
  }
}
