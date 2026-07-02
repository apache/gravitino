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
import java.util.Set;
import javax.annotation.Nullable;
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
   * @param requestContext authorization request context
   * @return authorization result.
   */
  boolean authorize(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext);

  boolean deny(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext);

  /**
   * Determine whether the user is the Owner of a certain metadata object.
   *
   * @param principal the user principal
   * @param metalake the metalake
   * @param metadataObject the metadataObject.
   * @param requestContext authorization request context
   * @return authorization result.
   */
  boolean isOwner(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      AuthorizationRequestContext requestContext);

  /**
   * Determine whether the user is the service admin.
   *
   * @return authorization result
   */
  boolean isServiceAdmin();

  /**
   * Determine whether the user accessing is oneself, or whether the group being accessed contains
   * oneself.
   *
   * @param type user or group
   * @param nameIdentifier name of user or group
   * @param requestContext authorization request context; enables per-request dedup with other
   *     authorization calls in the same request
   * @return authorization result
   */
  boolean isSelf(
      Entity.EntityType type,
      NameIdentifier nameIdentifier,
      AuthorizationRequestContext requestContext);

  /**
   * Determine whether the user is the metalake user.
   *
   * @param metalake metalake
   * @param requestContext authorization request context; enables per-request dedup with other
   *     authorizer calls (e.g. {@code authorize}/{@code isOwner}) that look up the same user.
   * @return authorization result
   */
  boolean isMetalakeUser(String metalake, AuthorizationRequestContext requestContext);

  /**
   * Determines whether the given principal may have any {@code DENY} policy, for any of the given
   * privileges, at any scope (metalake, catalog, schema or the object itself) within the metalake.
   *
   * <p>This supports list-authorization short-circuiting: when a privilege is granted at a parent
   * scope (metalake, catalog or schema), every child object is visible <em>unless</em> a {@code
   * DENY} overrides it at some scope. A {@code false} return therefore lets the caller skip
   * per-object authorization for the whole list and return every identifier; a {@code true} return
   * forces the caller to fall back to per-object authorization.
   *
   * <p>The query is scope-agnostic on purpose: a deny granted at a parent scope hides the whole
   * subtree, while a deny granted on a single object hides just that object. Both cases must
   * disable the short-circuit, so the privilege match is not restricted to a metadata type.
   *
   * <p>The default implementation conservatively returns {@code true} so that authorizers which
   * cannot answer the question never enable an unsafe short-circuit.
   *
   * @param principal the user principal
   * @param metalake the metalake
   * @param privileges the privileges whose denies would affect visibility
   * @param requestContext authorization request context
   * @return whether a deny for any of the given privileges may exist
   */
  default boolean hasDenyPolicy(
      Principal principal,
      String metalake,
      Set<Privilege.Name> privileges,
      AuthorizationRequestContext requestContext) {
    return true;
  }

  /**
   * Determine whether the user can set owner
   *
   * @param metalake metalake
   * @param type metadata type
   * @param fullName metadata full name
   * @param requestContext authorization request context
   * @return authorization result
   */
  boolean hasSetOwnerPermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext);

  /**
   * Determine whether the user can grant or revoke privilege for metadata
   *
   * @param metalake metalake
   * @param type metadata type
   * @param fullName metadata full name
   * @param requestContext authorization request context
   * @return authorization result
   */
  boolean hasMetadataPrivilegePermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext);

  /**
   * When the permissions of a role change, it is necessary to notify the GravitinoAuthorizer in
   * order to clear the cache.
   *
   * @param roleId The role id;
   */
  void handleRolePrivilegeChange(Long roleId);

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
   * Called when the role assignments of a user change.
   *
   * @param metalake the metalake name
   * @param userName the user name
   */
  default void handleUserRoleRelChange(String metalake, String userName) {
    // default no-op for backward compatibility
  }

  /**
   * Called when the role assignments of a group change.
   *
   * @param metalake the metalake name
   * @param groupName the group name
   */
  default void handleGroupRoleRelChange(String metalake, String groupName) {
    // default no-op for backward compatibility
  }

  /**
   * This method is called to clear the owner relationship in jcasbin when the owner of the metadata
   * changes.
   *
   * @param metalake metalake;
   * @param oldOwnerId The old owner id; null when setting the first owner.
   * @param nameIdentifier The metadata name identifier;
   * @param type entity type
   */
  void handleMetadataOwnerChange(
      String metalake,
      @Nullable Long oldOwnerId,
      NameIdentifier nameIdentifier,
      Entity.EntityType type);

  /**
   * Called when an entity name-to-id mapping may have changed because of a rename or drop.
   * Implementations evict the cache key for the given entity and all of its descendants.
   *
   * @param metalake the metalake name
   * @param nameIdentifier the entity name identifier
   * @param type the entity type
   */
  default void handleEntityNameIdMappingChange(
      String metalake, NameIdentifier nameIdentifier, Entity.EntityType type) {
    // default no-op for backward compatibility
  }
}
