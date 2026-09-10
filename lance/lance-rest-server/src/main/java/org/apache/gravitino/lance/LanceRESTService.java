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

import static org.apache.gravitino.lance.common.config.LanceConfig.NAMESPACE_BACKEND;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.Servlet;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceBackend;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
<<<<<<< HEAD
=======
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.lance.service.LanceHealthCheckPathMatcher;
import org.apache.gravitino.lance.service.LanceServiceIdentityFilter;
import org.apache.gravitino.lance.service.authorization.LanceAuthorizationMetadataFilter;
import org.apache.gravitino.lance.service.authorization.LanceRESTAuthInterceptionService;
>>>>>>> 7478ab48e ([#12975] fix(core): Preserve errors thrown by PrincipalUtils.doAs (#12976))
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.server.web.HttpServerMetricsSource;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.RequestContextFilter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Thin REST service shell for Lance metadata. */
public class LanceRESTService implements GravitinoAuxiliaryService {

  private static final Logger LOG = LoggerFactory.getLogger(LanceRESTService.class);

  public static final String SERVICE_NAME = "lance-rest";
  public static final String LANCE_SPEC = "/lance/*";

  private static final String LANCE_REST_SPEC_PACKAGE = "org.apache.gravitino.lance.service.rest";

  private JettyServer server;
  private NamespaceWrapper lanceNamespace;

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties, boolean auxMode) {
    LanceConfig lanceConfig = new LanceConfig(properties);
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(lanceConfig);

    server = new JettyServer();
    // Get MetricsSystem and EventBus from GravitinoEnv once at init time.
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    EventBus eventBus = GravitinoEnv.getInstance().eventBus();
    server.initialize(serverConfig, SERVICE_NAME, false);

    this.lanceNamespace = loadNamespaceImpl(lanceConfig);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(JacksonFeature.class);
    resourceConfig.packages(LANCE_REST_SPEC_PACKAGE);
    resourceConfig.register(LanceExceptionMapper.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(lanceNamespace).to(NamespaceWrapper.class).ranked(1);
          }
        });

    // Register metrics with shared MetricsSystem
    HttpServerMetricsSource httpServerMetricsSource =
        new HttpServerMetricsSource(
            MetricsSource.LANCE_REST_SERVER_METRIC_NAME, resourceConfig, server);
    metricsSystem.register(httpServerMetricsSource);

    Servlet container = new ServletContainer(resourceConfig);
    server.addServlet(container, LANCE_SPEC);
    // Registered before HttpAuditFilter so audit events dispatched during this request carry the
    // request's query parameters and remote address, exactly as on the main server.
    server.addFilter(new RequestContextFilter(eventBus), LANCE_SPEC);
    server.addFilter(
        new HttpAuditFilter(eventBus, EventSource.GRAVITINO_LANCE_REST_SERVER), LANCE_SPEC);
    server.addSystemFilters(LANCE_SPEC);

    registerMetricsPathFilters(server, eventBus);

    // Custom filters are registered once, across every filtered path in a single call, so a
    // filter whose init() isn't safe to run more than once per JVM only runs it once rather than
    // once per pathSpec.
    List<String> customFilterPaths = new ArrayList<>(JettyServer.METRICS_PATH_SPECS);
    customFilterPaths.add(LANCE_SPEC);
    server.addCustomFilters(customFilterPaths.toArray(new String[0]));

    LOG.info(
        "Initialized Lance REST service for backend {} in {} mode",
        lanceConfig.getNamespaceBackend(),
        auxMode ? "auxiliary" : "standalone");
  }

  @Override
  public void serviceStart() {
    if (server != null) {
      server.start();
      LOG.info("Lance REST service started");
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Lance REST service stopped");
    }
    if (lanceNamespace != null) {
      lanceNamespace.close();
    }
  }

  public void join() {
    if (server != null) {
      server.join();
    }
  }

  /**
   * Registers request-context tracking and audit-on-failure coverage on {@link
   * JettyServer#METRICS_PATH_SPECS}. {@code /metrics} and {@code /prometheus/metrics} used to
   * receive no such coverage at all, with nothing in the build catching it; {@code
   * RequestContextFilter} is included too so query-parameter capture applies uniformly, matching
   * {@link #LANCE_SPEC}. Package-private and static so a unit test can exercise it directly against
   * a plain {@link JettyServer}, without booting the rest of {@link #serviceInit}. See GH-12760.
   *
   * @param server the Jetty server whose {@link JettyServer#METRICS_PATH_SPECS} need filter
   *     coverage
   * @param eventBus the event bus audit events are dispatched through
   */
  static void registerMetricsPathFilters(JettyServer server, EventBus eventBus) {
    for (String pathSpec : JettyServer.METRICS_PATH_SPECS) {
      server.addFilter(new RequestContextFilter(eventBus), pathSpec);
      server.addFilter(
          new HttpAuditFilter(eventBus, EventSource.GRAVITINO_LANCE_REST_SERVER), pathSpec);
    }
  }

  private NamespaceWrapper loadNamespaceImpl(LanceConfig lanceConfig) {
    String backendType = lanceConfig.get(NAMESPACE_BACKEND);
    LanceNamespaceBackend lanceNamespaceBackend = LanceNamespaceBackend.fromType(backendType);

    try {
      Constructor<? extends NamespaceWrapper> constructor =
          lanceNamespaceBackend.getWrapperClass().getConstructor(LanceConfig.class);

      return constructor.newInstance(lanceConfig);
    } catch (Exception e) {
      LOG.error("Error loading namespace implementation for backend type: {}", backendType, e);
      throw new RuntimeException("Failed to load namespace implementation", e);
    }
  }
}
