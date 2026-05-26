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
package org.apache.gravitino.idp.dto.util;

import org.apache.gravitino.idp.dto.IdpGroupDTO;
import org.apache.gravitino.idp.dto.IdpUserDTO;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;

/** Utility class for converting between built-in IdP DTOs and domain objects. */
public class IdpDTOConverters {

  private IdpDTOConverters() {}

  /**
   * Converts a built-in IdP user to a user DTO.
   *
   * @param user The built-in IdP user.
   * @return The user DTO.
   */
  public static IdpUserDTO toDTO(IdpUser user) {
    return IdpUserDTO.builder().withName(user.name()).withGroups(user.groupNames()).build();
  }

  /**
   * Converts a built-in IdP group to a group DTO.
   *
   * @param group The built-in IdP group.
   * @return The group DTO.
   */
  public static IdpGroupDTO toDTO(IdpGroup group) {
    return IdpGroupDTO.builder().withName(group.name()).withUsers(group.usernames()).build();
  }
}
