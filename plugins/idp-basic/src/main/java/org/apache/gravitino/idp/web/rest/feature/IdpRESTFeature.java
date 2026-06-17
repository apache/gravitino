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
package org.apache.gravitino.idp.web.rest.feature;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.auth.BasicAuthenticator;
import org.apache.gravitino.idp.web.rest.IdpAuthorizationFilter;
import org.apache.gravitino.idp.web.rest.IdpBasicBinder;
import org.apache.gravitino.idp.web.rest.IdpGroupOperations;
import org.apache.gravitino.idp.web.rest.IdpUserOperations;
import org.apache.gravitino.server.authentication.Authenticator;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers built-in IdP REST resources for the idp-basic plugin.
 *
 * <p>Configure {@link Configs#REST_API_EXTENSION_PACKAGES} to {@link #IDP_REST_EXTENSION_PACKAGE}
 * and list {@code basic} in {@link Configs#AUTHENTICATORS} so Jersey auto-discovers this feature.
 * IdP REST resource classes remain in {@code org.apache.gravitino.idp.web.rest} and are registered
 * here. Also initializes configured service admins in the built-in IdP when they do not yet exist.
 */
@Provider
public class IdpRESTFeature implements Feature {

  private static final Logger LOG = LoggerFactory.getLogger(IdpRESTFeature.class);

  /** Extension package name registered through {@code gravitino.server.rest.extensionPackages}. */
  public static final String IDP_REST_EXTENSION_PACKAGE = IdpRESTFeature.class.getPackageName();

  /** Environment variable for the initial password of configured service admins. */
  public static final String INITIAL_ADMIN_PASSWORD_ENV = "GRAVITINO_INITIAL_ADMIN_PASSWORD";

  @Override
  public boolean configure(FeatureContext context) {
    GravitinoEnv env = GravitinoEnv.getInstance();
    Config config = env.config();
    validateConfiguration(config);
    registerBasicAuthenticator(config);

    try {
      IdpUserGroupManager manager = IdpUserGroupManager.getInstance(config, env.idGenerator());
      manager.initializeConfiguredServiceAdmins(config, System.getenv(INITIAL_ADMIN_PASSWORD_ENV));
      LOG.info("Initialized built-in IdP service admins");
    } catch (IOException e) {
      throw new IllegalStateException("Failed to initialize built-in IdP service admins", e);
    }

    context.register(IdpBasicBinder.class);
    context.register(IdpUserOperations.class);
    context.register(IdpGroupOperations.class);
    context.register(IdpAuthorizationFilter.class);
    return true;
  }

  /**
   * Validates that the server configuration is compatible with the built-in IdP plugin.
   *
   * <p>Called when the idp-basic extension package is enabled. Requires {@code basic} in {@link
   * Configs#AUTHENTICATORS} and rejects {@code simple}.
   *
   * @param config The server configuration.
   */
  static void validateConfiguration(Config config) {
    List<String> authenticators = config.get(Configs.AUTHENTICATORS);
    boolean usesBasic =
        authenticators.stream()
            .anyMatch(name -> AuthenticatorType.BASIC.name().equalsIgnoreCase(name.trim()));
    boolean usesSimple =
        authenticators.stream()
            .anyMatch(name -> AuthenticatorType.SIMPLE.name().equalsIgnoreCase(name.trim()));

    if (!usesBasic) {
      LOG.error(
          "gravitino.server.rest.extensionPackages includes the built-in IdP plugin ({}) but "
              + "'basic' is not listed in gravitino.authenticators. Add 'basic' to "
              + "gravitino.authenticators.",
          IDP_REST_EXTENSION_PACKAGE);
      System.exit(1);
    }

    if (usesSimple) {
      LOG.error(
          "Built-in IdP basic authentication is incompatible with simple authentication because "
              + "both handle Authorization: Basic headers. Remove 'simple' from "
              + "gravitino.authenticators.");
      System.exit(1);
    }
  }

  private static void registerBasicAuthenticator(Config config) {
    List<Authenticator> authenticators = ServerAuthenticator.getInstance().authenticators();
    if (authenticators == null) {
      return;
    }
    for (Authenticator authenticator : authenticators) {
      if (authenticator instanceof BasicAuthenticator) {
        return;
      }
    }
    BasicAuthenticator basicAuthenticator = new BasicAuthenticator();
    basicAuthenticator.initialize(config);
    authenticators.add(0, basicAuthenticator);
  }
}
