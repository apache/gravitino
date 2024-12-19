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
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
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
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param role The name of the role.
   * @param entity The name of the entity.
   * @param privileges The list of privileges.
   */
  public RevokePrivilegesFromRole(
      String url,
      boolean ignoreVersions,
      String metalake,
      String role,
      FullName entity,
      String[] privileges) {
    super(url, ignoreVersions);
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
      List<Privilege> privilegesList = new ArrayList<>();

      for (String privilege : privileges) {
        if (!Privileges.isValid(privilege)) {
          System.err.println("Unknown privilege " + privilege);
          return;
        }
        PrivilegeDTO privilegeDTO =
            PrivilegeDTO.builder()
                .withName(Privileges.toName(privilege))
                .withCondition(Privilege.Condition.DENY)
                .build();
        privilegesList.add(privilegeDTO);
      }

      MetadataObject metadataObject = constructMetadataObject(entity, client);
      client.revokePrivilegesFromRole(role, metadataObject, privilegesList);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchRoleException err) {
      System.err.println(ErrorMessages.UNKNOWN_ROLE);
      return;
    } catch (NoSuchMetadataObjectException err) {
      System.err.println(ErrorMessages.UNKNOWN_USER);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    String all = String.join(",", privileges);
    System.out.println(role + " revoked " + all + " on " + entity.getName());
  }
}
