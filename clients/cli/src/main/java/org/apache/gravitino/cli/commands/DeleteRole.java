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

import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

public class DeleteRole extends Command {

  protected String metalake;
  protected String role;
  protected boolean force;

  /**
   * Delete a role.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param force Force operation.
   * @param metalake The name of the metalake.
   * @param role The name of the role.
   */
  public DeleteRole(
      String url, boolean ignoreVersions, boolean force, String metalake, String role) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.force = force;
    this.role = role;
  }

  /** Delete a role. */
  @Override
  public void handle() {
    boolean deleted = false;

    if (!AreYouSure.really(force)) {
      return;
    }

    try {
      GravitinoClient client = buildClient(metalake);
      deleted = client.deleteRole(role);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (deleted) {
      System.out.println(role + " deleted.");
    } else {
      System.out.println(role + " not deleted.");
    }
  }
}
