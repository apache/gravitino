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

import java.util.ArrayList;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;

/** Removes a role from a user. */
public class RemoveRoleFromUser extends Command {

  protected String metalake;
  protected String user;
  protected String role;

  /**
   * Removes a role from a user.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param user The name of the user.
   * @param role The name of the role.
   */
  public RemoveRoleFromUser(CommandContext context, String metalake, String user, String role) {
    super(context);
    this.metalake = metalake;
    this.user = user;
    this.role = role;
  }

  /** Removes a role from a user. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      ArrayList<String> roles = new ArrayList<>();
      roles.add(role);
      client.revokeRolesFromUser(roles, user);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (NoSuchUserException err) {
      exitWithError(ErrorMessages.UNKNOWN_USER);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(role + " removed from " + user);
  }
}
