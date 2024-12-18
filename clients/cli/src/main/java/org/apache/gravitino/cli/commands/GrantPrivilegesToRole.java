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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.cli.Privileges;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

/** Grants one or more privileges. */
public class GrantPrivilegesToRole extends Command {

  protected final String metalake;
  protected final String role;
  protected final FullName entity;
  protected final String[] privileges;

  /**
   * Grants one or more privileges.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param role The name of the role.
   * @param entity The name of the entity.
   * @param privileges The list of privileges.
   */
  public GrantPrivilegesToRole(
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

  /** Grants one or more privileges. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      MetadataObject metadataObject = null;
      List<Privilege> privilegesList = new ArrayList<>();
      String name = entity.getName();

      for (String privilege : privileges) {
        if (!Privileges.isValid(privilege)) {
          System.err.println("Unknown privilege " + privilege);
          return;
        }
        PrivilegeDTO privilegeDTO =
            PrivilegeDTO.builder()
                .withName(Privileges.toName(privilege))
                .withCondition(Privilege.Condition.ALLOW)
                .build();
        privilegesList.add(privilegeDTO);
      }

      if (entity.hasColumnName()) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.COLUMN);
      } else if (entity.hasTableName()) {
        Catalog catalog = client.loadCatalog(entity.getCatalogName());
        Catalog.Type catalogType = catalog.type();
        if (catalogType == Catalog.Type.RELATIONAL) {
          metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.TABLE);
        } else if (catalogType == Catalog.Type.MESSAGING) {
          metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.TOPIC);
        } else if (catalogType == Catalog.Type.FILESET) {
          metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.FILESET);
        } else {
          throw new IllegalArgumentException("Unknown entity type: " + name);
        }
      } else if (entity.hasSchemaName()) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.SCHEMA);
      } else if (entity.hasCatalogName()) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.CATALOG);
      } else if (entity.getMetalakeName() != null) {
        metadataObject = MetadataObjects.of(null, name, MetadataObject.Type.METALAKE);
      } else {
        throw new IllegalArgumentException("Unknown entity type: " + name);
      }

      client.grantPrivilegesToRole(role, metadataObject, privilegesList);
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
    System.out.println(role + " granted " + all + " on " + entity.getName());
  }
}
