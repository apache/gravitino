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

/** Represents a built-in IdP group Data Transfer Object (DTO). */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode
@ToString
public class IdpGroupDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("users")
  @JsonSetter(nulls = Nulls.AS_EMPTY)
  private List<String> users = Collections.emptyList();

  /**
   * Creates a new instance of IdpGroupDTO.
   *
   * @param name The name of the built-in IdP group DTO.
   * @param users The users of the built-in IdP group DTO.
   */
  @Builder(setterPrefix = "with")
  protected IdpGroupDTO(String name, List<String> users) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
    if (users != null) {
      users.forEach(
          user ->
              Preconditions.checkArgument(
                  StringUtils.isNotBlank(user), "users cannot contain null or empty user names"));
    }
    this.name = name;
    this.users = users == null ? Collections.emptyList() : users;
  }

  /**
   * @return The name of the built-in IdP group DTO.
   */
  public String name() {
    return name;
  }

  /**
   * The users of the built-in IdP group. A group can contain multiple users.
   *
   * @return The users of the built-in IdP group.
   */
  public List<String> users() {
    return users;
  }
}
