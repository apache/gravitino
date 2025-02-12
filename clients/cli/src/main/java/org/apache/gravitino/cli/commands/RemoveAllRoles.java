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

package org.apache.gravitino.cli.commands;

import java.util.List;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;

/** Removes all roles from a group or user. */
public class RemoveAllRoles extends Command {
  protected final String metalake;
  protected final String entity;
  protected final String entityType;

  /**
   * Removes all roles from a group or user.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param entity the name of the group or user.
   * @param entityType The type of the entity (group or user).
   */
  public RemoveAllRoles(CommandContext context, String metalake, String entity, String entityType) {
    super(context);
    this.metalake = metalake;
    this.entity = entity;
    this.entityType = entityType;
  }

  /** Removes all roles from a group or user. */
  @Override
  public void handle() {
    if (CommandEntities.GROUP.equals(entityType)) {
      revokeAllRolesFromGroup();
    } else {
      revokeAllRolesFromUser();
    }
  }

  /** Removes all roles from a group. */
  private void revokeAllRolesFromGroup() {
    List<String> roles;
    try {
      GravitinoClient client = buildClient(metalake);
      Group group = client.getGroup(entity);
      roles = group.roles();
      client.revokeRolesFromGroup(roles, entity);
    } catch (NoSuchMetalakeException e) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchGroupException e) {
      exitWithError(ErrorMessages.UNKNOWN_GROUP);
    } catch (Exception e) {
      exitWithError(e.getMessage());
    }

    printInformation("All roles have been revoked from group " + entity);
  }

  /** Removes all roles from a user. */
  private void revokeAllRolesFromUser() {
    List<String> roles;
    try {
      GravitinoClient client = buildClient(metalake);
      User user = client.getUser(entity);
      roles = user.roles();
      client.revokeRolesFromUser(roles, entity);
    } catch (NoSuchMetalakeException e) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchUserException e) {
      exitWithError(ErrorMessages.UNKNOWN_USER);
    } catch (Exception e) {
      exitWithError(e.getMessage());
    }

    printInformation("All roles have been revoked from user " + entity);
  }
}
