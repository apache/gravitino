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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a list of tag names. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class TagNameListResponse extends NameListResponse {

  @JsonProperty("hasInaccessibleTags")
  private final boolean hasInaccessibleTags;

  /**
   * Creates a new TagNameListResponse.
   *
   * @param names The list of accessible tag names.
   * @param hasInaccessibleTags Whether inaccessible tags were omitted from the response.
   */
  public TagNameListResponse(String[] names, boolean hasInaccessibleTags) {
    super(names);
    this.hasInaccessibleTags = hasInaccessibleTags;
  }

  /** Constructor used by Jackson deserializer. */
  public TagNameListResponse() {
    super();
    this.hasInaccessibleTags = false;
  }

  /**
   * Returns whether inaccessible tags were omitted from the response.
   *
   * @return true if at least one inaccessible tag was omitted, false otherwise
   */
  public boolean hasInaccessibleTags() {
    return hasInaccessibleTags;
  }
}
