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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Lists;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.junit.jupiter.api.Test;

class TestIdpRESTFeature {

  @Test
  void testSimpleFails() {
    Config config = newConfig(AuthenticatorType.SIMPLE.name().toLowerCase());

    SystemExitException exception =
        assertThrows(SystemExitException.class, () -> validateWithExitGuard(config));

    assertEquals(1, exception.status());
  }

  @Test
  void testDefaultSimpleFails() {
    Config config = new Config(false) {};

    assertThrows(SystemExitException.class, () -> validateWithExitGuard(config));
  }

  @Test
  void testOAuthOk() {
    Config config = newConfig(AuthenticatorType.OAUTH.name().toLowerCase());

    assertDoesNotThrow(() -> IdpRESTFeature.validateConfiguration(config));
  }

  private static Config newConfig(String... authenticators) {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Lists.newArrayList(authenticators));
    return config;
  }

  @SuppressWarnings("removal")
  private static void validateWithExitGuard(Config config) {
    SecurityManager original = System.getSecurityManager();
    System.setSecurityManager(
        new SecurityManager() {
          @Override
          public void checkExit(int status) {
            throw new SystemExitException(status);
          }

          @Override
          public void checkPermission(java.security.Permission perm) {
            // Allow test execution.
          }
        });
    try {
      IdpRESTFeature.validateConfiguration(config);
    } finally {
      System.setSecurityManager(original);
    }
  }

  private static final class SystemExitException extends SecurityException {
    private final int status;

    private SystemExitException(int status) {
      super("System.exit(" + status + ")");
      this.status = status;
    }

    private int status() {
      return status;
    }
  }
}
