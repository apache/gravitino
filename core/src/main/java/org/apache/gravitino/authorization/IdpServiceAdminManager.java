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

package org.apache.gravitino.authorization;

import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.local.ServiceAdminManager;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.apache.gravitino.utils.PrincipalUtils;

/** The service admin manager backed by the built-in IdP user store. */
public class IdpServiceAdminManager implements ServiceAdminManager {

  private final IdpUserManager userManager;
  private final IdpUserMetaService userMetaService;

  /**
   * Create a service admin manager from the current environment.
   *
   * @return The service admin manager.
   */
  public static IdpServiceAdminManager fromEnvironment() {
    return new IdpServiceAdminManager(
        IdpUserManager.fromEnvironment(), IdpUserMetaService.getInstance());
  }

  IdpServiceAdminManager(IdpUserManager userManager, IdpUserMetaService userMetaService) {
    this.userManager = userManager;
    this.userMetaService = userMetaService;
  }

  @Override
  public boolean serviceAdminExists(String userName) {
    return userMetaService.findUser(userName).isPresent();
  }

  @Override
  public void initializeServiceAdmin(String userName, String password) {
    try {
      PrincipalUtils.doAs(
          new UserPrincipal(userName),
          () -> {
            userManager.createUser(userName, password);
            return null;
          });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Failed to initialize service admin %s", userName), e);
    }
  }
}
