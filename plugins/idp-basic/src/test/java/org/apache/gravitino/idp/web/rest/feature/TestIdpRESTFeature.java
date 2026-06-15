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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.collect.Lists;
import javax.ws.rs.core.FeatureContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.web.rest.IdpAuthorizationFilter;
import org.apache.gravitino.idp.web.rest.IdpBasicBinder;
import org.apache.gravitino.idp.web.rest.IdpGroupOperations;
import org.apache.gravitino.idp.web.rest.IdpUserOperations;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestIdpRESTFeature {

  private Config originalConfig;
  private IdGenerator originalIdGenerator;

  @BeforeEach
  void saveGravitinoEnvConfig() throws IllegalAccessException {
    GravitinoEnv env = GravitinoEnv.getInstance();
    originalConfig = (Config) FieldUtils.readField(env, "config", true);
    originalIdGenerator = (IdGenerator) FieldUtils.readField(env, "idGenerator", true);
  }

  @AfterEach
  void restoreGravitinoEnvConfig() throws IllegalAccessException {
    GravitinoEnv env = GravitinoEnv.getInstance();
    FieldUtils.writeField(env, "config", originalConfig, true);
    FieldUtils.writeField(env, "idGenerator", originalIdGenerator, true);
  }

  @Test
  void testSimpleSkips() throws IllegalAccessException {
    configureWithAuthenticators(AuthenticatorType.SIMPLE.name().toLowerCase());
    assertConfigureSkipsIdp();
  }

  @Test
  void testDefaultSimpleSkips() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", new Config(false) {}, true);
    assertConfigureSkipsIdp();
  }

  @Test
  void testSimpleWithOAuthSkips() throws IllegalAccessException {
    configureWithAuthenticators(
        AuthenticatorType.OAUTH.name().toLowerCase(),
        AuthenticatorType.SIMPLE.name().toLowerCase());
    assertConfigureSkipsIdp();
  }

  @Test
  void testOAuthRegisters() throws Exception {
    configureWithAuthenticators(AuthenticatorType.OAUTH.name().toLowerCase());
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", mock(IdGenerator.class), true);

    IdpUserGroupManager manager = mock(IdpUserGroupManager.class);
    try (MockedStatic<IdpUserGroupManager> ignored = mockStatic(IdpUserGroupManager.class)) {
      ignored
          .when(() -> IdpUserGroupManager.getInstance(any(Config.class), any(IdGenerator.class)))
          .thenReturn(manager);

      FeatureContext context = mock(FeatureContext.class);
      assertTrue(new IdpRESTFeature().configure(context));

      verify(manager).initializeConfiguredServiceAdmins(any(Config.class), nullable(String.class));
      verify(context).register(IdpBasicBinder.class);
      verify(context).register(IdpUserOperations.class);
      verify(context).register(IdpGroupOperations.class);
      verify(context).register(IdpAuthorizationFilter.class);
    }
  }

  private static void assertConfigureSkipsIdp() {
    FeatureContext context = mock(FeatureContext.class);
    assertTrue(new IdpRESTFeature().configure(context));
    verifyNoInteractions(context);
  }

  private static void configureWithAuthenticators(String... authenticators)
      throws IllegalAccessException {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Lists.newArrayList(authenticators));
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
  }
}
