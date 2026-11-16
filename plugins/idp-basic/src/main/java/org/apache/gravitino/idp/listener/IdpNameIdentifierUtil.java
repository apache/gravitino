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
 * Name identifiers for built-in IdP ({@code basic}) users and groups. These identities are
 * instance-scoped (not metalake-scoped). The first namespace segment is the authenticator / local
 * store channel ({@code basic}), parallel to other instance identity channels such as {@code
 * scim.user.<name>}.
 */
public final class IdpNameIdentifierUtil {

  /** Channel prefix for the built-in basic IdP local user store. */
  public static final String BASIC_CHANNEL = "basic";

  private IdpNameIdentifierUtil() {}

  /**
   * Creates a {@link NameIdentifier} for a built-in IdP user.
   *
   * @param userName the username
   * @return {@code basic.user.<userName>}
   */
  public static NameIdentifier ofUser(String userName) {
    return NameIdentifier.of(BASIC_CHANNEL, Entity.USER_SCHEMA_NAME, userName);
  }

  /**
   * Creates a {@link NameIdentifier} for a built-in IdP group.
   *
   * @param groupName the group name
   * @return {@code basic.group.<groupName>}
   */
  public static NameIdentifier ofGroup(String groupName) {
    return NameIdentifier.of(BASIC_CHANNEL, Entity.GROUP_SCHEMA_NAME, groupName);
  }
}
