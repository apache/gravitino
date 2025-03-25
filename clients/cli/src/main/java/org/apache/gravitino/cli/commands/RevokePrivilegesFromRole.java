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

import java.util.HashSet;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.cli.Privileges;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

/** Revokes one or more privileges. */
public class RevokePrivilegesFromRole extends MetadataCommand {

  protected final String metalake;
  protected final String role;
  protected final FullName entity;
  protected final String[] privileges;

  /**
   * Revokes one or more privileges.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param role The name of the role.
   * @param entity The name of the entity.
   * @param privileges The list of privileges.
   */
  public RevokePrivilegesFromRole(
      CommandContext context, String metalake, String role, FullName entity, String[] privileges) {
    super(context);
    this.metalake = metalake;
    this.entity = entity;
    this.role = role;
    this.privileges = privileges;
  }

  /** Revokes One or more privileges. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      Set<Privilege> privilegesSet = new HashSet<>();

      for (String privilege : privileges) {
        if (!Privileges.isValid(privilege)) {
          exitWithError(ErrorMessages.UNKNOWN_PRIVILEGE + " " + privilege);
        }
        PrivilegeDTO privilegeDTO =
            PrivilegeDTO.builder()
                .withName(Privileges.toName(privilege))
                .withCondition(Privilege.Condition.DENY)
                .build();
        privilegesSet.add(privilegeDTO);
      }

      MetadataObject metadataObject = constructMetadataObject(entity, client);
      client.revokePrivilegesFromRole(role, metadataObject, privilegesSet);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException err) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (NoSuchMetadataObjectException err) {
      exitWithError(ErrorMessages.UNKNOWN_USER);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    String all = String.join(",", privileges);
    printInformation(role + " revoked " + all + " on " + entity.getName());
  }

  @Override
  public Command validate() {
    if (privileges == null) exitWithError(ErrorMessages.MISSING_PRIVILEGES);
    return super.validate();
  }
}
