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

import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

public class RoleAudit extends AuditCommand {

  protected final String metalake;
  protected final String role;

  /**
   * Displays the audit information of a role.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param role The name of the role.
   */
  public RoleAudit(CommandContext context, String metalake, String role) {
    super(context);
    this.metalake = metalake;
    this.role = role;
  }

  /** Displays the audit information of a specified role. */
  @Override
  public void handle() {
    Role result = null;

    try (GravitinoClient client = buildClient(metalake)) {
      result = client.getRole(this.role);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (result != null) {
      displayAuditInfo(result.auditInfo());
    }
  }
}
