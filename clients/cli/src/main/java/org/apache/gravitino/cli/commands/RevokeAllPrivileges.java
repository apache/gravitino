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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

/** Revokes all privileges from a role on the selected entity. */
public class RevokeAllPrivileges extends MetadataCommand {

  /** The name of the metalake. */
  protected final String metalake;
  /** The name of the role. */
  protected final String role;
  /** The full name of the entity. */
  protected final FullName entity;

  /**
   * Revokes all privileges from a role on the selected entity.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param role The name of the role.
   * @param entity The name of the entity.
   */
  public RevokeAllPrivileges(
      CommandContext context, String metalake, String role, FullName entity) {
    super(context);
    this.metalake = metalake;
    this.role = role;
    this.entity = entity;
  }

  /** Revokes all privileges from a role on the selected entity. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      List<SecurableObject> matchedObjects = getMatchedObjects(client);

      if (!matchedObjects.isEmpty()) {
        MetadataObject target = constructMetadataObject(entity, client);
        Set<Privilege> privileges =
            matchedObjects.stream()
                .filter(object -> object.type() == target.type())
                .flatMap(object -> object.privileges().stream())
                .collect(Collectors.toSet());
        if (!privileges.isEmpty()) {
          client.revokePrivilegesFromRole(role, target, privileges);
          printInformation("Revoked privileges:");
          printResults(
              target.fullName()
                  + ": "
                  + privileges.stream()
                      .map(Privilege::simpleString)
                      .collect(Collectors.joining(", ")));
          return;
        }
      }
      printInformation("No privileges revoked.");
    } catch (NoSuchMetalakeException e) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException e) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (Exception e) {
      exitWithError(e.getMessage());
    }
  }

  private List<SecurableObject> getMatchedObjects(GravitinoClient client) {
    Role gRole = client.getRole(role);
    return gRole.securableObjects().stream()
        .filter(s -> s.fullName().equals(entity.getName()))
        .collect(Collectors.toList());
  }

  /**
   * verify the arguments.
   *
   * @return Returns itself via argument validation, otherwise exits.
   */
  @Override
  public Command validate() {
    if (!entity.hasName()) exitWithError(ErrorMessages.MISSING_NAME);
    return super.validate();
  }
}
