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

/** Adds a role to a group. */
public class AddRoleToGroup extends Command {

  protected String metalake;
  protected String group;
  protected String role;

  /**
   * Adds a role to a group.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param group The name of the group.
   * @param role The name of the role.
   */
  public AddRoleToGroup(CommandContext context, String metalake, String group, String role) {
    super(context);
    this.metalake = metalake;
    this.group = group;
    this.role = role;
  }

  /** Adds a role to a group. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      ArrayList<String> roles = new ArrayList<>();
      roles.add(role);
      client.grantRolesToGroup(roles, group);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (NoSuchUserException err) {
      exitWithError(ErrorMessages.UNKNOWN_USER);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(role + " added to " + group);
  }
}
