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
package org.apache.gravitino.connector.authorization;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.AuthorizationPluginException;

/**
 * Interface for authorization User and Group plugin operation of the underlying access control
 * system.
 */
interface UserGroupAuthorizationPlugin {
  /**
   * After adding a User to Gravitino, this method is called to add the User to the underlying
   * system. <br>
   *
   * @param user The user entity.
   * @return True if the add User was successfully added, false if the add User failed.
   * @throws AuthorizationPluginException If adding the User encounters storage issues.
   */
  Boolean onUserAdded(User user) throws AuthorizationPluginException;

  /**
   * After removing a User from Gravitino, this method is called to remove the User from the
   * underlying system. <br>
   *
   * @param user The user entity.
   * @return True if the User was successfully removed, false if the remove User failed.
   * @throws AuthorizationPluginException If removing the User encounters storage issues.
   */
  Boolean onUserRemoved(User user) throws AuthorizationPluginException;

  /**
   * After acquiring a User from Gravitino, this method is called to acquire the User in the
   * underlying system. <br>
   * Because User information is already stored in the Gravitino, so we don't need to get the User
   * from the underlying access control system. <br>
   * We only need to check if the User exists in the underlying access control system.
   *
   * @param user The user entity.
   * @return IF exist return true, else return false.
   * @throws AuthorizationPluginException If getting the User encounters underlying access control
   *     system issues.
   */
  Boolean onUserAcquired(User user) throws AuthorizationPluginException;

  /**
   * After adding a Group to Gravitino, this method is called to add the Group to the underlying
   * system. <br>
   *
   * @param group The group entity.
   * @return True if the add Group was successfully added, false if the add Group failed.
   * @throws AuthorizationPluginException If adding the Group encounters storage issues.
   */
  Boolean onGroupAdded(Group group) throws AuthorizationPluginException;

  /**
   * After removing a Group from Gravitino, this method is called to remove the Group from the
   * underlying system. <br>
   *
   * @param group The group entity.
   * @return True if the remove Group was successfully removed, false if the remove Group was
   *     failed.
   * @throws AuthorizationPluginException If removing the Group encounters storage issues.
   */
  Boolean onGroupRemoved(Group group) throws AuthorizationPluginException;

  /**
   * After acquiring a Group from Gravitino, this method is called to acquire the Group in the
   * underlying system. <br>
   * Because Group information is already stored in the Gravitino, so we don't need to get the Group
   * from the underlying access control system. <br>
   * We only need to check if the Group exists in the underlying access control system. <br>
   *
   * @param group The group entity.
   * @return If exist return true, else return false.
   * @throws AuthorizationPluginException If getting the Group encounters underlying access control
   *     system issues.
   */
  Boolean onGroupAcquired(Group group) throws AuthorizationPluginException;

  /**
   * After set a Owner to Gravitino, this method is called to set the Owner to the underlying
   * system. <br>
   *
   * @param metadataObject The metadata entity.
   * @param preOwner The previous owner.
   * @param newOwner The new owner.
   * @return True if the set Owner was successfully set, false if the set Owner failed.
   * @throws AuthorizationPluginException If adding the Group encounters storage issues.
   */
  Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException;
}
