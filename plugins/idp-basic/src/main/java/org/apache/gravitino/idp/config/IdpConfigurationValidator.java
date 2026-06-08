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

package org.apache.gravitino.idp.config;

import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validates server configuration before the built-in IdP plugin starts. */
public final class IdpConfigurationValidator {

  private static final Logger LOG = LoggerFactory.getLogger(IdpConfigurationValidator.class);

  private IdpConfigurationValidator() {}

  /**
   * Validates that the server configuration is compatible with the built-in IdP plugin.
   *
   * @param config The server configuration.
   */
  public static void validate(Config config) {
    if (!config.get(Configs.ENABLE_AUTHORIZATION)) {
      return;
    }
    boolean usesSimple =
        config.get(Configs.AUTHENTICATORS).stream()
            .anyMatch(name -> AuthenticatorType.SIMPLE.name().equalsIgnoreCase(name.trim()));
    if (usesSimple) {
      LOG.error(
          "Built-in IdP cannot be used with Simple authentication when authorization is enabled. "
              + "Remove 'simple' from gravitino.authenticators (default is simple), "
              + "or disable gravitino.authorization.enable.");
      System.exit(1);
    }
  }
}
