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
package org.apache.gravitino.lance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.service.LanceServiceIdentityFilter;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.JettyServerTestUtils;
import org.apache.gravitino.server.web.RequestContextFilter;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestLanceRESTService {

  private Config previousConfig;
  private MetricsSystem previousMetricsSystem;
  private EventBus previousEventBus;

  @AfterEach
  public void tearDown() throws Exception {
    if (previousConfig != null) {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    }
    if (previousMetricsSystem != null) {
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "metricsSystem", previousMetricsSystem, true);
    }
    if (previousEventBus != null) {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "eventBus", previousEventBus, true);
    }
  }

  private void injectGravitinoEnv(boolean authorizationEnabled) throws Exception {
    previousConfig = (Config) FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousMetricsSystem =
        (MetricsSystem) FieldUtils.readField(GravitinoEnv.getInstance(), "metricsSystem", true);
    previousEventBus =
        (EventBus) FieldUtils.readField(GravitinoEnv.getInstance(), "eventBus", true);

    Config mockConfig = mock(Config.class);
    when(mockConfig.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(authorizationEnabled);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "metricsSystem", new MetricsSystem(), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "eventBus", new EventBus(Collections.emptyList()), true);
  }

  private boolean hasServiceIdentityFilter(LanceRESTService service) throws Exception {
    JettyServer server = (JettyServer) FieldUtils.readField(service, "server", true);
    ServletHandler servletHandler =
        JettyServerTestUtils.getServletContextHandler(server).getServletHandler();
    Set<String> filterPathSpecs =
        JettyServerTestUtils.filterPathSpecsFor(servletHandler, LanceServiceIdentityFilter.class);
    return !filterPathSpecs.isEmpty();
  }

  /** See GH-13093. The filter must not be registered when authorization is enabled. */
  @Test
  public void testServiceIdentityFilterNotRegisteredWhenAuthorizationEnabled() throws Exception {
    injectGravitinoEnv(true);

    Map<String, String> properties = new HashMap<>();
    properties.put(LanceConfig.CONFIG_NAMESPACE_BACKEND, LanceConfig.GRAVITINO_NAMESPACE_BACKEND);
    LanceRESTService service = new LanceRESTService();
    try {
      service.serviceInit(properties, true);
      assertFalse(
          hasServiceIdentityFilter(service),
          "LanceServiceIdentityFilter must not be registered when authorization is enabled");
    } finally {
      service.serviceStop();
    }
  }

  /** Filter is installed for backward compat when authorization is disabled. */
  @Test
  public void testServiceIdentityFilterRegisteredWhenAuthorizationDisabled() throws Exception {
    injectGravitinoEnv(false);

    Map<String, String> properties = new HashMap<>();
    properties.put(LanceConfig.CONFIG_NAMESPACE_BACKEND, LanceConfig.GRAVITINO_NAMESPACE_BACKEND);
    LanceRESTService service = new LanceRESTService();
    try {
      service.serviceInit(properties, true);
      assertTrue(
          hasServiceIdentityFilter(service),
          "LanceServiceIdentityFilter must be registered when authorization is disabled");
    } finally {
      service.serviceStop();
    }
  }

  /**
   * LanceRESTService.serviceInit() previously registered /metrics and /prometheus/metrics (added by
   * JettyServer#initialize() itself, outside LANCE_SPEC) with no audit coverage at all, and nothing
   * in the build caught it. Rather than parse source text, this exercises the extracted
   * LanceRESTService#registerMetricsPathFilters against a plain JettyServer and inspects the real
   * ServletHandler filter mappings it produces, so a broken JettyServer.METRICS_PATH_SPECS list or
   * a filter that merely appears in a comment cannot pass. See GH-12760.
   */
  @Test
  public void testMetricsPathsHaveAuditFilterCoverage() throws Exception {
    JettyServer server = new JettyServer();
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(new LanceConfig());
    server.initialize(jettyServerConfig, "test-lance-rest", false);
    EventBus eventBus = new EventBus(Collections.emptyList());

    try {
      LanceRESTService.registerMetricsPathFilters(server, eventBus);

      ServletHandler servletHandler =
          JettyServerTestUtils.getServletContextHandler(server).getServletHandler();
      Set<String> auditedPathSpecs =
          JettyServerTestUtils.filterPathSpecsFor(servletHandler, HttpAuditFilter.class);
      Set<String> requestContextPathSpecs =
          JettyServerTestUtils.filterPathSpecsFor(servletHandler, RequestContextFilter.class);

      for (String pathSpec : JettyServer.METRICS_PATH_SPECS) {
        assertTrue(
            auditedPathSpecs.contains(pathSpec),
            "'" + pathSpec + "' must be covered by HttpAuditFilter, see GH-12760");
        assertTrue(
            requestContextPathSpecs.contains(pathSpec),
            "'"
                + pathSpec
                + "' must be covered by RequestContextFilter for query-parameter "
                + "capture, see GH-12760");
      }
    } finally {
      server.stop();
    }
  }
}
