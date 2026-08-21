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
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** A policy selector that matches one exact tag assignment value. */
@Evolving
public final class TagValuePolicySelector implements PolicySelector {

  /** The selector type for exact tag assignment value matching. */
  public static final String TYPE = "TAG_VALUE";

  private final String value;

  private TagValuePolicySelector(String value) {
    this.value = value;
  }

  /**
   * Creates a selector that matches one exact tag assignment value.
   *
   * @param value The tag assignment value to match.
   * @return The selector.
   */
  public static TagValuePolicySelector of(String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "Selector value cannot be blank");
    return new TagValuePolicySelector(value);
  }

  @Override
  public String type() {
    return TYPE;
  }

  /**
   * @return The exact tag assignment value matched by this selector.
   */
  public String value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagValuePolicySelector)) {
      return false;
    }
    TagValuePolicySelector that = (TagValuePolicySelector) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return "TagValuePolicySelector{value='" + value + "'}";
  }
}
