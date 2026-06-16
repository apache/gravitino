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

package org.apache.gravitino.auth;

import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;

/** Validates built-in IdP Basic authentication configuration for the Gravitino server. */
public final class IdpBasicAuthConfigValidator {

  /** Jersey extension package that registers the built-in IdP REST feature. */
  public static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";

  private IdpBasicAuthConfigValidator() {}

  /**
   * Returns whether {@code basic} is listed in {@link Configs#AUTHENTICATORS}.
   *
   * @param authenticators configured authenticator names
   * @return {@code true} when built-in IdP Basic authentication is requested
   */
  public static boolean usesBasicAuthenticator(List<String> authenticators) {
    return authenticators.stream()
        .anyMatch(name -> AuthenticatorType.BASIC.name().equalsIgnoreCase(name.trim()));
  }

  /**
   * Returns whether the built-in IdP REST extension package is registered.
   *
   * @param extensionPackages configured REST extension packages
   * @return {@code true} when the idp-basic plugin is enabled
   */
  public static boolean hasIdpExtensionPackage(List<String> extensionPackages) {
    return extensionPackages.stream()
        .anyMatch(pkg -> IDP_REST_EXTENSION_PACKAGE.equals(pkg.trim()));
  }

  /**
   * Validates that built-in IdP Basic authentication is configured consistently.
   *
   * <p>Built-in IdP Basic authentication is enabled only when {@code basic} is listed in {@link
   * Configs#AUTHENTICATORS} and {@link #IDP_REST_EXTENSION_PACKAGE} is listed in {@link
   * Configs#REST_API_EXTENSION_PACKAGES}. The two settings must be present together. {@code basic}
   * is incompatible with {@code simple}.
   *
   * @param config server configuration
   * @throws IllegalArgumentException when the configuration is invalid
   */
  public static void validate(Config config) {
    List<String> authenticators = config.get(Configs.AUTHENTICATORS);
    List<String> extensionPackages = config.get(Configs.REST_API_EXTENSION_PACKAGES);
    boolean usesBasic = usesBasicAuthenticator(authenticators);
    boolean hasIdpExtension = hasIdpExtensionPackage(extensionPackages);
    boolean usesSimple =
        authenticators.stream()
            .anyMatch(name -> AuthenticatorType.SIMPLE.name().equalsIgnoreCase(name.trim()));

    if (usesSimple && usesBasic) {
      throw new IllegalArgumentException(
          "Built-in IdP basic authentication is incompatible with simple authentication because "
              + "both handle Authorization: Basic headers. Remove either 'simple' or 'basic' from "
              + "gravitino.authenticators.");
    }

    if (usesBasic && !hasIdpExtension) {
      throw new IllegalArgumentException(
          "'basic' is listed in gravitino.authenticators but gravitino.server.rest.extensionPackages "
              + "does not include "
              + IDP_REST_EXTENSION_PACKAGE
              + ". Add the extension package or remove 'basic' from gravitino.authenticators.");
    }

    if (hasIdpExtension && !usesBasic) {
      throw new IllegalArgumentException(
          "gravitino.server.rest.extensionPackages includes the built-in IdP plugin ("
              + IDP_REST_EXTENSION_PACKAGE
              + ") but 'basic' is not listed in gravitino.authenticators. Add 'basic' to "
              + "gravitino.authenticators or remove the extension package.");
    }
  }
}
