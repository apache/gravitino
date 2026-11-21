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
package org.apache.gravitino.authorization.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;

/**
 * Abstraction for querying Gravitino RBAC role assignments, role definitions, ownership relations,
 * and catalog names needed to compute minimal idempotent Kafka ACL deltas.
 */
public interface KafkaAuthorizationStateProvider {

  /**
   * Resolves a catalog's current name within a metalake by its entity ID.
   *
   * @param metalake Metalake name.
   * @param catalogId Catalog entity ID, or {@code null} if unknown.
   * @return Optional catalog name if resolvable.
   */
  Optional<String> resolveCatalogName(String metalake, @Nullable Long catalogId);

  /**
   * Lists user names currently holding the specified role.
   *
   * @param metalake Metalake name.
   * @param roleName Role name.
   * @return List of user names bound to the role.
   */
  List<String> listUserNamesByRole(String metalake, String roleName);

  /**
   * Lists group names currently holding the specified role.
   *
   * @param metalake Metalake name.
   * @param roleName Role name.
   * @return List of group names bound to the role.
   */
  List<String> listGroupNamesByRole(String metalake, String roleName);

  /**
   * Loads all full {@link Role} definitions (including securable objects and privileges) currently
   * granted to a user.
   *
   * @param metalake Metalake name.
   * @param username User name.
   * @return List of full role definitions held by the user.
   */
  List<Role> listRolesForUser(String metalake, String username);

  /**
   * Loads all full {@link Role} definitions (including securable objects and privileges) currently
   * granted to a group.
   *
   * @param metalake Metalake name.
   * @param groupName Group name.
   * @return List of full role definitions held by the group.
   */
  List<Role> listRolesForGroup(String metalake, String groupName);

  /**
   * Lists metadata objects (scoped to the metalake and optional Kafka catalog) currently owned by
   * the specified user or group.
   *
   * @param metalake Metalake name.
   * @param catalogName Optional catalog name to restrict catalog/schema/topic scan, or {@code
   *     null}.
   * @param ownerType Principal type ({@link Owner.Type#USER} or {@link Owner.Type#GROUP}).
   * @param ownerName Principal name.
   * @return Owned metadata objects.
   */
  List<MetadataObject> listOwnedObjectsForPrincipal(
      String metalake, @Nullable String catalogName, Owner.Type ownerType, String ownerName);

  /**
   * Lists all user names registered in the metalake.
   *
   * @param metalake Metalake name.
   * @return User names in the metalake.
   */
  default List<String> listAllUserNames(String metalake) {
    return Collections.emptyList();
  }

  /**
   * Lists all group names registered in the metalake.
   *
   * @param metalake Metalake name.
   * @return Group names in the metalake.
   */
  default List<String> listAllGroupNames(String metalake) {
    return Collections.emptyList();
  }

  /**
   * Returns whether the specified user or group currently exists in the metalake.
   *
   * @param metalake Metalake name.
   * @param principalType Principal type ({@link Owner.Type#USER} or {@link Owner.Type#GROUP}).
   * @param principalName Principal name.
   * @return {@code true} if the principal is registered in Gravitino.
   */
  default boolean principalExists(String metalake, Owner.Type principalType, String principalName) {
    return true;
  }
}
