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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** Represents one tag assignment pair in tag association requests. */
@Evolving
public final class TagValuePair {

  private final String name;

  @Nullable private final String value;

  /**
   * Creates a tag value pair.
   *
   * @param name The tag name.
   * @param value The assignment value. Null means a valueless tag assignment.
   */
  @JsonCreator
  public TagValuePair(
      @JsonProperty("name") String name, @Nullable @JsonProperty("value") String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Creates a valued tag assignment pair.
   *
   * @param name The tag name.
   * @param value The assignment value.
   * @return The tag value pair.
   */
  public static TagValuePair of(String name, String value) {
    return new TagValuePair(name, value);
  }

  /**
   * Creates a valueless tag assignment pair.
   *
   * @param name The tag name.
   * @return The tag value pair.
   */
  public static TagValuePair valueless(String name) {
    return new TagValuePair(name, null);
  }

  /**
   * @return The tag name.
   */
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  /**
   * @return The tag name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The assignment value, or null for valueless assignment pairs.
   */
  @Nullable
  @JsonProperty("value")
  public String getValue() {
    return value;
  }

  /**
   * @return The optional assignment value.
   */
  public Optional<String> value() {
    return Optional.ofNullable(value);
  }

  /**
   * Compares this pair with another object.
   *
   * @param o The object to compare.
   * @return True if the object is equal to this pair.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagValuePair)) {
      return false;
    }

    TagValuePair that = (TagValuePair) o;
    return Objects.equals(name, that.name) && Objects.equals(value, that.value);
  }

  /**
   * @return The hash code of this tag value pair.
   */
  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  /**
   * @return The string representation of this tag value pair.
   */
  @Override
  public String toString() {
    return "TagValuePair{" + "name='" + name + '\'' + ", value='" + value + '\'' + '}';
  }
}
