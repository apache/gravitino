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

package org.apache.gravitino.server.authorization;

import java.io.Closeable;
import java.security.Principal;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;

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
  void handleRolePrivilegeChange(Long roleId);
}
