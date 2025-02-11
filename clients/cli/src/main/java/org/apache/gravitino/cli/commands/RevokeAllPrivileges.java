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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

/** Revokes all privileges from a role to an entity or all entities. */
public class RevokeAllPrivileges extends MetadataCommand {

  protected final String metalake;
  protected final String role;
  protected final FullName entity;

  /**
   * Revokes all privileges from a role to an entity or all entities.
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

  /** Revokes all privileges from a role to an entity or all entities. */
  @Override
  public void handle() {
    List<SecurableObject> matchedObjects;
    Map<String, Set<Privilege>> revokedPrivileges = Maps.newHashMap();

    try {
      GravitinoClient client = buildClient(metalake);
      matchedObjects = getMatchedObjects(client);

      for (SecurableObject securableObject : matchedObjects) {
        String objectFullName = securableObject.fullName();
        Set<Privilege> privileges = new HashSet<>(securableObject.privileges());
        MetadataObject metadataObject = constructMetadataObject(entity, client);
        client.revokePrivilegesFromRole(role, metadataObject, privileges);

        revokedPrivileges.put(objectFullName, privileges);
      }
    } catch (NoSuchMetalakeException e) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchRoleException e) {
      exitWithError(ErrorMessages.UNKNOWN_ROLE);
    } catch (Exception e) {
      exitWithError(e.getMessage());
    }

    if (revokedPrivileges.isEmpty()) {
      printInformation("No privileges revoked.");
    } else {
      outputRevokedPrivileges(revokedPrivileges);
    }
  }

  private List<SecurableObject> getMatchedObjects(GravitinoClient client) {
    Role gRole = client.getRole(role);
    return gRole.securableObjects().stream()
        .filter(s -> s.fullName().equals(entity.getName()))
        .collect(Collectors.toList());
  }

  private void outputRevokedPrivileges(Map<String, Set<Privilege>> revokedPrivileges) {
    List<String> revokedInfoList = Lists.newArrayList();

    for (Map.Entry<String, Set<Privilege>> entry : revokedPrivileges.entrySet()) {
      List<String> revokedPrivilegesList =
          entry.getValue().stream().map(Privilege::simpleString).collect(Collectors.toList());
      revokedInfoList.add(entry.getKey() + ": " + COMMA_JOINER.join(revokedPrivilegesList));
    }

    printInformation("Revoked privileges:");
    for (String info : revokedInfoList) {
      printResults(info);
    }
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
