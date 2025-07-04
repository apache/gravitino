/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.authorization;

import java.io.Closeable;
import java.security.Principal;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Used for metadata authorization. */
public interface GravitinoAuthorizer extends Closeable {

  /**
   * After instantiating the GravitinoAuthorizer, execute the initialize method to perform a series
   * of initialization operations, such as loading privilege policies and so on.
   */
  void initialize();

  /**
   * Perform authorization and return the authorization result.
   *
   * @param principal the user principal
   * @param metalake the metalake
   * @param metadataObject the metadataObject.
   * @param privilege for example, CREATE_CATALOG, CREATE_TABLE, etc.
   * @return authorization result.
   */
  boolean authorize(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege);

  /**
   * Determine whether the user is the Owner of a certain metadata object.
   *
   * @param principal the user principal
   * @param metalake the metalake
   * @param metadataObject the metadataObject.
   * @return authorization result.
   */
  boolean isOwner(Principal principal, String metalake, MetadataObject metadataObject);

  /**
   * When the permissions of a role change, it is necessary to notify the GravitinoAuthorizer in
   * order to clear the cache.
   *
   * @param roleId The role id;
   */
  default void handleRolePrivilegeChange(Long roleId) {};

  /**
   * When the permissions of a role change, it is necessary to notify the GravitinoAuthorizer in
   * order to clear the cache.
   *
   * @param metalake The metalake name;
   * @param roleName The role name;
   */
  default void handleRolePrivilegeChange(String metalake, String roleName) {
    try {
      RoleEntity entity =
          GravitinoEnv.getInstance()
              .entityStore()
              .get(
                  NameIdentifierUtil.ofRole(metalake, roleName),
                  Entity.EntityType.ROLE,
                  RoleEntity.class);
      handleRolePrivilegeChange(entity.id());
    } catch (Exception e) {
      throw new RuntimeException("Can not get Role Entity", e);
    }
  }

  /**
   * This method is called to clear the owner relationship in jcasbin when the owner of the metadata
   * changes.
   *
   * @param metalake metalake;
   * @param oldOwnerId The old owner id;
   * @param nameIdentifier The metadata name identifier;
   * @param type entity type
   */
  default void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {};
}
