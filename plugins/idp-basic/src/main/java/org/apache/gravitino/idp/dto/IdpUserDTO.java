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

package org.apache.gravitino.idp.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a built-in IdP user Data Transfer Object (DTO). */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode
@ToString
public class IdpUserDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("groups")
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  private List<String> groups = Collections.emptyList();

  /**
   * Creates a new instance of IdpUserDTO.
   *
   * @param name The name of the built-in IdP user DTO.
   * @param groups The groups of the built-in IdP user DTO.
   */
  @Builder(setterPrefix = "with")
  protected IdpUserDTO(String name, List<String> groups) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
    if (groups != null) {
      groups.forEach(
          group ->
              Preconditions.checkArgument(
                  StringUtils.isNotBlank(group),
                  "groups cannot contain null or empty group names"));
    }
    this.name = name;
    this.groups = groups == null ? Collections.emptyList() : groups;
  }

  /**
   * @return The name of the built-in IdP user DTO.
   */
  public String name() {
    return name;
  }

  /**
   * The groups of the built-in IdP user. A user can belong to multiple groups.
   *
   * @return The groups of the built-in IdP user.
   */
  public List<String> groups() {
    return groups;
  }
}
