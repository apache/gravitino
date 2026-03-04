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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

/** Represents delete a role */
public class DeleteRole extends Command {
  /** The joiner for the comma separated roles. */
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();
  /** The name of the metalake. */
  protected String metalake;
  /** The array of the role. */
  protected String[] roles;
  /** The flag to force the deletion. */
  protected boolean force;

  /**
   * Delete a role.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param roles The name of the role.
   */
  public DeleteRole(CommandContext context, String metalake, String[] roles) {
    super(context);
    this.metalake = metalake;
    this.force = context.force();
    this.roles = roles;
  }

  /** Delete a role. */
  @Override
  public void handle() {
    if (!AreYouSure.really(force)) {
      return;
    }
    List<String> failedRoles = Lists.newArrayList();
    List<String> successRoles = Lists.newArrayList();

    try {
      GravitinoClient client = buildClient(metalake);
      for (String role : roles) {
        (client.deleteRole(role) ? successRoles : failedRoles).add(role);
      }
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (failedRoles.isEmpty()) {
      printInformation(COMMA_JOINER.join(successRoles) + " deleted.");
    } else {
      printInformation(
          COMMA_JOINER.join(successRoles)
              + " deleted, "
              + "but "
              + COMMA_JOINER.join(failedRoles)
              + " is not deleted.");
    }
  }
}
