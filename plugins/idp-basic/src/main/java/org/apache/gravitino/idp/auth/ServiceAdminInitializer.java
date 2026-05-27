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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.idp.basic.IdpCredentialValidator;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.POConverters;

/** Initializes configured service admins in the built-in IdP during server startup. */
public final class ServiceAdminInitializer {

  static final String INITIAL_ADMIN_PASSWORD_ENV = "GRAVITINO_INITIAL_ADMIN_PASSWORD";

  private static final String BASIC_AUTHENTICATOR = AuthenticatorType.BASIC.name().toLowerCase();

  private ServiceAdminInitializer() {}

  /**
   * Initialize the service admins using the current runtime environment.
   *
   * @param config The configuration object to initialize the service admins.
   */
  public static void initialize(Config config) throws IOException {
    initialize(
        config,
        IdpUserMetaService.getInstance(),
        PasswordHasherFactory.create(),
        GravitinoEnv.getInstance().idGenerator(),
        System.getenv(INITIAL_ADMIN_PASSWORD_ENV));
  }

  static void initialize(
      Config config,
      IdpUserMetaService userMetaService,
      PasswordHasher passwordHasher,
      IdGenerator idGenerator,
      @Nullable String initialAdminPasswords)
      throws IOException {
    List<String> authenticators = config.get(Configs.AUTHENTICATORS);
    if (authenticators == null || !authenticators.contains(BASIC_AUTHENTICATOR)) {
      return;
    }

    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || serviceAdmins.isEmpty()) {
      return;
    }

    Map<String, String> passwordsByAdmin =
        parseInitialAdminPasswords(serviceAdmins, initialAdminPasswords);
    for (String serviceAdmin : serviceAdmins) {
      IdpCredentialValidator.validateUsername(serviceAdmin);
      if (userExists(userMetaService, serviceAdmin)) {
        continue;
      }

      String password = passwordsByAdmin.get(serviceAdmin);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(password),
          "Missing initial password for configured service admin %s; declare %s",
          serviceAdmin,
          INITIAL_ADMIN_PASSWORD_ENV);
      userMetaService.insertIdpUser(
          newServiceAdminUser(idGenerator, passwordHasher, serviceAdmin, password));
    }
  }

  private static IdpUserPO newServiceAdminUser(
      IdGenerator idGenerator, PasswordHasher passwordHasher, String username, String password) {
    return IdpUserPO.builder()
        .withUserId(idGenerator.nextId())
        .withUsername(username)
        .withPasswordHash(passwordHasher.hash(password))
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  private static boolean userExists(IdpUserMetaService userMetaService, String username) {
    try {
      userMetaService.getIdpUserByUsername(username);
      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  private static Map<String, String> parseInitialAdminPasswords(
      List<String> serviceAdmins, @Nullable String initialAdminPassword) {
    if (StringUtils.isBlank(initialAdminPassword)) {
      return ImmutableMap.of();
    }

    IdpCredentialValidator.validatePassword(initialAdminPassword);
    ImmutableMap.Builder<String, String> passwordsByAdmin = ImmutableMap.builder();
    for (String serviceAdmin : serviceAdmins) {
      passwordsByAdmin.put(serviceAdmin, initialAdminPassword);
    }
    return passwordsByAdmin.build();
  }
}
