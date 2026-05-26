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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.json.JsonUtils;
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
    if (!enabledBasicAuthenticator(config)) {
      return;
    }

    List<String> serviceAdmins = configuredServiceAdmins(config);
    if (serviceAdmins.isEmpty()) {
      return;
    }

    Map<String, String> initialPasswords =
        parseInitialAdminPasswords(serviceAdmins, initialAdminPasswords);
    for (String serviceAdmin : serviceAdmins) {
      validateUserName(serviceAdmin);
      if (userMetaService.idpUserExists(serviceAdmin)) {
        continue;
      }

      String password = initialPasswords.get(serviceAdmin);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(password),
          "Missing initial password for configured service admin %s; declare %s",
          serviceAdmin,
          INITIAL_ADMIN_PASSWORD_ENV);
      userMetaService.insertIdpUser(
          newUserPO(serviceAdmin, passwordHasher.hash(password), idGenerator));
    }
  }

  private static IdpUserPO newUserPO(
      String username, String passwordHash, IdGenerator idGenerator) {
    return IdpUserPO.builder()
        .withUserId(idGenerator.nextId())
        .withUsername(username)
        .withPasswordHash(passwordHash)
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  private static boolean enabledBasicAuthenticator(Config config) {
    return config.get(Configs.AUTHENTICATORS).contains(BASIC_AUTHENTICATOR);
  }

  private static List<String> configuredServiceAdmins(Config config) {
    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || serviceAdmins.isEmpty()) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(serviceAdmins);
  }

  private static Map<String, String> parseInitialAdminPasswords(
      List<String> serviceAdmins, @Nullable String initialAdminPasswords) {
    if (StringUtils.isBlank(initialAdminPasswords)) {
      return ImmutableMap.of();
    }

    final List<String> entries;
    try {
      entries =
          JsonUtils.objectMapper()
              .readValue(initialAdminPasswords, new TypeReference<List<String>>() {});
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          INITIAL_ADMIN_PASSWORD_ENV + " must be a JSON array of 'username:password' strings", e);
    }

    Map<String, String> passwordsByAdmin = new LinkedHashMap<>();
    for (String entry : entries) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(entry),
          "%s must not contain blank entries",
          INITIAL_ADMIN_PASSWORD_ENV);

      int separatorIndex = entry.indexOf(':');
      Preconditions.checkArgument(
          separatorIndex > 0,
          "%s entry '%s' must use the format username:password",
          INITIAL_ADMIN_PASSWORD_ENV,
          entry);

      String userName = entry.substring(0, separatorIndex);
      String password = entry.substring(separatorIndex + 1);
      validateUserName(userName);
      validatePassword(password);
      Preconditions.checkArgument(
          serviceAdmins.contains(userName),
          "%s entry '%s' is not a configured service admin",
          INITIAL_ADMIN_PASSWORD_ENV,
          userName);
      Preconditions.checkArgument(
          !passwordsByAdmin.containsKey(userName),
          "%s contains duplicate entries for service admin %s",
          INITIAL_ADMIN_PASSWORD_ENV,
          userName);
      passwordsByAdmin.put(userName, password);
    }

    return ImmutableMap.copyOf(passwordsByAdmin);
  }

  private static void validateUserName(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName), "User name is required");
    Preconditions.checkArgument(!userName.contains(":"), "User name cannot contain ':'");
  }

  private static void validatePassword(String password) {
    Preconditions.checkArgument(StringUtils.isNotBlank(password), "Password is required");
    Preconditions.checkArgument(
        password.length() >= 12, "Password length must be at least 12 characters");
    Preconditions.checkArgument(
        password.length() <= 64, "Password length must be at most 64 characters");
  }
}
