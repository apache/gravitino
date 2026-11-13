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
package org.apache.gravitino.dto.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.tag.TagValue;

/** A backing tag name and optional exact assignment value. */
@Getter
public class DomainBackingTagDTO {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("value")
  @Nullable
  private final String value;

  /** Creates a backing tag for Jackson deserialization. */
  public DomainBackingTagDTO() {
    this(null, null);
  }

  /**
   * Creates a backing tag representation.
   *
   * @param name The tag name.
   * @param value The optional value; null matches any assignment.
   */
  public DomainBackingTagDTO(String name, @Nullable String value) {
    this.name = name;
    this.value = value;
  }

  /** Validates the name and optional value. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "backingTag.name must not be blank");
    Preconditions.checkArgument(
        value == null || StringUtils.isNotBlank(value), "backingTag.value must not be blank");
    Preconditions.checkArgument(
        value == null || value.length() <= 256, "backingTag.value must not exceed 256 characters");
  }

  /**
   * @return The corresponding tag value.
   */
  public TagValue tagValue() {
    validate();
    return value == null ? TagValue.noValue(name) : TagValue.of(name, value);
  }
}
