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

package org.apache.gravitino.idp.auth;

import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.IdGenerator;

/** The service admin manager backed by the built-in IdP user store. */
public class IdpServiceAdminManager implements ServiceAdminManager {

  private static final long INITIAL_VERSION = 1L;

  private final IdpUserMetaService userMetaService;
  private final PasswordHasher passwordHasher;
  private final IdGenerator idGenerator;

  /**
   * Create a service admin manager from the current environment.
   *
   * @return The service admin manager.
   */
  public static IdpServiceAdminManager fromEnvironment() {
    return new IdpServiceAdminManager(
        IdpUserMetaService.getInstance(),
        PasswordHasherFactory.create(),
        GravitinoEnv.getInstance().idGenerator());
  }

  IdpServiceAdminManager(
      IdpUserMetaService userMetaService, PasswordHasher passwordHasher, IdGenerator idGenerator) {
    this.userMetaService = userMetaService;
    this.passwordHasher = passwordHasher;
    this.idGenerator = idGenerator;
  }

  @Override
  public boolean serviceAdminExists(String userName) {
    return userMetaService.idpUserExists(userName);
  }

  @Override
  public void initializeServiceAdmin(String userName, String password) {
    if (userMetaService.idpUserExists(userName)) {
      return;
    }

    IdpUserPO userPO =
        IdpUserPO.builder()
            .withUserId(idGenerator.nextId())
            .withUsername(userName)
            .withPasswordHash(passwordHasher.hash(password))
            .withCurrentVersion(INITIAL_VERSION)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    userMetaService.insertIdpUser(userPO);
  }
}
