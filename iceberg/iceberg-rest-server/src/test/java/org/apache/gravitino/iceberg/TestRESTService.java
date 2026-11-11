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
package org.apache.gravitino.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.ServiceLoader;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.restriction.IcebergReadRestrictionLoader;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRESTService {

  @Test
  @SuppressWarnings("unchecked")
  void testDiscoveryWarnsOnlyWhenInstalledProviderIsDisabled() {
    IcebergReadRestrictionLoader loader = mock(IcebergReadRestrictionLoader.class);
    ServiceLoader<IcebergReadRestrictionLoader> providers = mock(ServiceLoader.class);
    when(providers.iterator()).thenAnswer(ignored -> Collections.singletonList(loader).iterator());
    Logger logger = mock(Logger.class);
    try (MockedStatic<ServiceLoader> discovery =
            mockStatic(ServiceLoader.class, CALLS_REAL_METHODS);
        MockedStatic<LoggerFactory> logging = mockStatic(LoggerFactory.class, CALLS_REAL_METHODS)) {
      discovery
          .when(() -> ServiceLoader.load(IcebergReadRestrictionLoader.class))
          .thenReturn(providers);
      logging
          .when(() -> LoggerFactory.getLogger(IcebergReadRestrictionLoader.class))
          .thenReturn(logger);

      assertSame(loader, RESTService.loadReadRestrictionLoader(false));
      verify(logger).warn(contains("installed but disabled"), eq(loader.getClass().getName()));

      clearInvocations(logger);
      when(loader.enabled()).thenReturn(true);
      assertSame(loader, RESTService.loadReadRestrictionLoader(true));
      verifyNoInteractions(logger);
    }
  }

  @Test
  void testNoProviderDoesNotRequireAuthorizationOrWarn() {
    Logger logger = mock(Logger.class);
    try (MockedStatic<LoggerFactory> logging =
        mockStatic(LoggerFactory.class, CALLS_REAL_METHODS)) {
      logging
          .when(() -> LoggerFactory.getLogger(IcebergReadRestrictionLoader.class))
          .thenReturn(logger);

      assertFalse(RESTService.loadReadRestrictionLoader(false).enabled());
      verifyNoInteractions(logger);
    }
  }

  @Test
  void testReadRestrictionStartupRequiresAuthorization() {
    IcebergReadRestrictionLoader loader = mock(IcebergReadRestrictionLoader.class);
    when(loader.enabled()).thenReturn(true);
    GravitinoEnv environment = mock(GravitinoEnv.class);
    Config config = mock(Config.class);
    when(environment.config()).thenReturn(config);
    when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(false);
    try (MockedStatic<IcebergReadRestrictionLoader> discovery =
            mockStatic(IcebergReadRestrictionLoader.class);
        MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      discovery.when(IcebergReadRestrictionLoader::load).thenReturn(loader);
      environments.when(GravitinoEnv::getInstance).thenReturn(environment);

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () -> new RESTService().serviceInit(Collections.emptyMap(), true));
      assertEquals(
          "Iceberg read restrictions require authorization; set gravitino.authorization.enable=true",
          exception.getMessage());
      assertSame(loader, RESTService.loadReadRestrictionLoader(true));
    }
  }

  @Test
  void testDisabledReadRestrictionLoaderDoesNotRequireAuthorization() {
    IcebergReadRestrictionLoader loader = mock(IcebergReadRestrictionLoader.class);
    when(loader.enabled()).thenReturn(false);
    try (MockedStatic<IcebergReadRestrictionLoader> discovery =
        mockStatic(IcebergReadRestrictionLoader.class)) {
      discovery.when(IcebergReadRestrictionLoader::load).thenReturn(loader);

      assertSame(loader, RESTService.loadReadRestrictionLoader(false));
      assertSame(loader, RESTService.loadReadRestrictionLoader(true));
    }
  }

  /** The documented Iceberg REST key reaches the value the service passes to its error builder. */
  @Test
  public void testIncludeErrorStackTraceReadFromServiceConfig() {
    assertTrue(JettyServerConfig.fromConfig(new IcebergConfig()).isIncludeErrorStackTrace());
    assertFalse(
        JettyServerConfig.fromConfig(
                new IcebergConfig(Collections.singletonMap("includeErrorStackTrace", "false")))
            .isIncludeErrorStackTrace());
  }

  @Test
  public void testMetricsAreEnabledOnlyForConfiguredStandaloneMode() {
    IcebergConfig disabledConfig = new IcebergConfig();
    assertFalse(RESTService.shouldEnableMetrics(false, disabledConfig));
    assertFalse(RESTService.shouldEnableMetrics(true, disabledConfig));

    IcebergConfig enabledConfig =
        new IcebergConfig(Collections.singletonMap(IcebergConfig.METRICS_ENABLED.getKey(), "true"));
    assertTrue(RESTService.shouldEnableMetrics(false, enabledConfig));
    assertFalse(RESTService.shouldEnableMetrics(true, enabledConfig));
  }
}
