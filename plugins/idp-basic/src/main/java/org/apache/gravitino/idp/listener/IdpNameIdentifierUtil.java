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

package org.apache.gravitino.idp.listener;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;

/**
 * Name identifiers for built-in IdP users and groups. IdP identities are instance-scoped (not
 * metalake-scoped), so they use {@link Entity#SYSTEM_METALAKE_RESERVED_NAME} as the root namespace
 * segment rather than a metalake name.
 */
public final class IdpNameIdentifierUtil {

  private IdpNameIdentifierUtil() {}

  /**
   * Creates a {@link NameIdentifier} for a built-in IdP user.
   *
   * @param userName the username
   * @return {@code system.user.<userName>}
   */
  public static NameIdentifier ofUser(String userName) {
    return NameIdentifier.of(
        Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.USER_SCHEMA_NAME, userName);
  }

  /**
   * Creates a {@link NameIdentifier} for a built-in IdP group.
   *
   * @param groupName the group name
   * @return {@code system.group.<groupName>}
   */
  public static NameIdentifier ofGroup(String groupName) {
    return NameIdentifier.of(
        Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME, groupName);
  }
}
