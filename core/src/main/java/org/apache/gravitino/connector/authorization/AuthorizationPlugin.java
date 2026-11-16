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

import java.io.Closeable;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.AuthorizationPluginException;

/**
 * Authorization operations plugin interfaces. <br>
 * Notice: Because each interface function needs to perform multiple steps in the underlying
 * permission system, the implementation method of these function interface must be idempotent.
 */
public interface AuthorizationPlugin
    extends UserGroupAuthorizationPlugin,
        RoleAuthorizationPlugin,
        MetadataAuthorizationPlugin,
        Closeable {

  /**
   * Invoked when a user is removed from a Gravitino metalake so plugins that materialize grants
   * directly per principal (without deleting external identity accounts) can revoke managed grants.
   *
   * <p>Defaults to a no-op returning {@link Boolean#TRUE} so plugins whose {@link
   * #onUserRemoved(User)} drops external database/global accounts are unaffected by metalake
   * membership removal.
   *
   * @param user The user being removed from the metalake.
   * @return {@code true} if cleanup succeeded.
   * @throws AuthorizationPluginException If revoking principal grants fails.
   */
  default Boolean onMetalakeUserRemoved(User user) throws AuthorizationPluginException {
    return Boolean.TRUE;
  }

  /**
   * Invoked when a group is removed from a Gravitino metalake so plugins that materialize grants
   * directly per principal (without deleting external identity accounts) can revoke managed grants.
   *
   * <p>Defaults to a no-op returning {@link Boolean#TRUE} so plugins whose {@link
   * #onGroupRemoved(Group)} drops external database/global accounts are unaffected by metalake
   * membership removal.
   *
   * @param group The group being removed from the metalake.
   * @return {@code true} if cleanup succeeded.
   * @throws AuthorizationPluginException If revoking principal grants fails.
   */
  default Boolean onMetalakeGroupRemoved(Group group) throws AuthorizationPluginException {
    return Boolean.TRUE;
  }
}
