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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Singleton;
import javax.servlet.Servlet;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.IcebergAuthenticationFilter;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergHealthCheckPathMatcher;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupJobStore;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupManager;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceHookDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableHookDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewHookDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationExecutor;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProviderFactory;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.server.web.HealthAliasServlet;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.server.web.HttpServerMetricsSource;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.RequestContextFilter;
import org.apache.gravitino.server.web.filter.IcebergRESTAuthInterceptionService;
import org.glassfish.hk2.api.InterceptionService;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTService implements GravitinoAuxiliaryService {

  private static Logger LOG = LoggerFactory.getLogger(RESTService.class);

  private JettyServer server;

  public static final String SERVICE_NAME = "iceberg-rest";
  public static final String ICEBERG_SPEC = "/iceberg/*";
  private static final String ICEBERG_REST_SPEC_PACKAGE =
      "org.apache.gravitino.iceberg.service.rest";

  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;
  private IcebergMetricsManager icebergMetricsManager;
  private Optional<IcebergCleanupManager> cleanupManager;
  private IcebergConfigProvider configProvider;
  private boolean auxMode;

  private void initServer(IcebergConfig icebergConfig) {
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(icebergConfig);
    server =
        new JettyServer() {
          @Override
          protected javax.servlet.Filter createAuthenticationFilter() {
            return new IcebergAuthenticationFilter();
          }
        };
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    server.initialize(serverConfig, SERVICE_NAME, false /* shouldEnableUI */);

    ResourceConfig config = new ResourceConfig();
    config.packages(getIcebergRESTPackages(icebergConfig));

    config.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    config.register(IcebergExceptionMapper.class);
    HttpServerMetricsSource httpServerMetricsSource =
        new HttpServerMetricsSource(MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME, config, server);
    metricsSystem.register(httpServerMetricsSource);

    Map<String, String> configProperties = icebergConfig.getAllConfig();
    this.configProvider = IcebergConfigProviderFactory.create(configProperties);
    configProvider.initialize(configProperties);
    String metalakeName = configProvider.getMetalakeName();
    boolean skipAuthorizationForRestBackend =
        icebergConfig.get(IcebergConfig.ICEBERG_REST_DISABLE_REST_AUTHZ);

    Boolean enableAuth = GravitinoEnv.getInstance().config().get(Configs.ENABLE_AUTHORIZATION);
    EventBus eventBus = GravitinoEnv.getInstance().eventBus();
    this.icebergCatalogWrapperManager =
        new IcebergCatalogWrapperManager(configProperties, configProvider, auxMode, metalakeName);
    IcebergRESTServerContext authorizationContext =
        IcebergRESTServerContext.create(
            configProvider,
            enableAuth,
            auxMode,
            skipAuthorizationForRestBackend,
            icebergCatalogWrapperManager);
    this.icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    if (auxMode) {
      // Async cleanup reuses the entity store's shared relational backend (connection pool +
      // per-backend SQL), which is only available when running embedded in the Gravitino server
      // (auxiliary mode). In standalone mode the cleanup manager stays empty and purge requests
      // fall back to synchronous purge.
      this.cleanupManager =
          Optional.of(
              new IcebergCleanupManager(
                  new IcebergCleanupJobStore(GravitinoEnv.getInstance().idGenerator()),
                  icebergConfig));
    } else {
      this.cleanupManager = Optional.empty();
      LOG.info(
          "Async Iceberg table cleanup is only available in auxiliary mode; "
              + "purge requests with async mode will fall back to synchronous purge.");
    }

    // The raw namespace operation executor is shared with the table and view hook dispatchers so
    // their orphan-schema cleanup can probe namespace existence without firing namespace events.
    IcebergNamespaceOperationDispatcher namespaceOperationDispatcher =
        new IcebergNamespaceOperationExecutor(icebergCatalogWrapperManager, cleanupManager);

    // Table: EventDispatcher -> HookDispatcher -> OperationExecutor
    IcebergTableOperationDispatcher icebergTableOperationDispatcher =
        new IcebergTableOperationExecutor(icebergCatalogWrapperManager, cleanupManager);
    if (authorizationContext.isAuthorizationEnabled()) {
      icebergTableOperationDispatcher =
          new IcebergTableHookDispatcher(
              icebergTableOperationDispatcher, namespaceOperationDispatcher);
    }
    IcebergTableOperationDispatcher icebergTableDispatcher =
        new IcebergTableEventDispatcher(icebergTableOperationDispatcher, eventBus, metalakeName);

    // View: EventDispatcher -> HookDispatcher -> OperationExecutor
    IcebergViewOperationDispatcher icebergViewOperationDispatcher =
        new IcebergViewOperationExecutor(icebergCatalogWrapperManager);
    if (authorizationContext.isAuthorizationEnabled()) {
      icebergViewOperationDispatcher =
          new IcebergViewHookDispatcher(
              icebergViewOperationDispatcher, namespaceOperationDispatcher, metalakeName);
    }
    IcebergViewOperationDispatcher icebergViewDispatcher =
        new IcebergViewEventDispatcher(icebergViewOperationDispatcher, eventBus, metalakeName);

    // Namespace: EventDispatcher -> HookDispatcher -> OperationExecutor
    if (authorizationContext.isAuthorizationEnabled()) {
      namespaceOperationDispatcher =
          new IcebergNamespaceHookDispatcher(namespaceOperationDispatcher);
    }
    IcebergNamespaceOperationDispatcher icebergNamespaceDispatcher =
        new IcebergNamespaceEventDispatcher(namespaceOperationDispatcher, eventBus, metalakeName);

    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            if (authorizationContext.isAuthorizationEnabled()) {
              bind(IcebergRESTAuthInterceptionService.class)
                  .to(InterceptionService.class)
                  .in(Singleton.class);
            }
            bind(icebergCatalogWrapperManager).to(IcebergCatalogWrapperManager.class).ranked(1);
            bind(icebergMetricsManager).to(IcebergMetricsManager.class).ranked(1);
            cleanupManager.ifPresent(
                manager -> bind(manager).to(IcebergCleanupManager.class).ranked(1));
            bind(icebergTableDispatcher).to(IcebergTableOperationDispatcher.class).ranked(1);
            bind(icebergViewDispatcher).to(IcebergViewOperationDispatcher.class).ranked(1);
            bind(icebergNamespaceDispatcher)
                .to(IcebergNamespaceOperationDispatcher.class)
                .ranked(1);
          }
        });

    Servlet servlet = new ServletContainer(config);
    server.addServlet(servlet, ICEBERG_SPEC);
    // Registered before HttpAuditFilter so audit events dispatched during this request carry the
    // request's query parameters and remote address, exactly as on the main server.
    server.addFilter(new RequestContextFilter(eventBus), ICEBERG_SPEC);
    server.addFilter(
        new HttpAuditFilter(
            eventBus,
            EventSource.GRAVITINO_ICEBERG_REST_SERVER,
            new IcebergHealthCheckPathMatcher()),
        ICEBERG_SPEC);
    server.addSystemFilters(ICEBERG_SPEC);

    // Root-level aliases for health checks to improve compatibility with various monitoring
    // systems that expect a /health endpoint. Not part of JettyServer.METRICS_PATH_SPECS below:
    // HealthAliasServlet forwards every request into /iceberg/health*, which ICEBERG_SPEC already
    // covers via the servlet container's FORWARD dispatcher type, so binding the filter again
    // here would double-log every probe.
    server.addServlet(new HealthAliasServlet("/iceberg"), "/health/*");
    server.addServlet(new HealthAliasServlet("/iceberg"), "/health.html");

    registerMetricsPathFilters(server, eventBus);

    // Custom filters are registered once, across every filtered path in a single call, so a
    // filter whose init() isn't safe to run more than once per JVM only runs it once rather than
    // once per pathSpec.
    List<String> customFilterPaths = new ArrayList<>(JettyServer.METRICS_PATH_SPECS);
    customFilterPaths.add(ICEBERG_SPEC);
    server.addCustomFilters(customFilterPaths.toArray(new String[0]));
  }

  /**
   * Registers request-context tracking and audit-on-failure coverage on {@link
   * JettyServer#METRICS_PATH_SPECS}. {@code /metrics} and {@code /prometheus/metrics} used to
   * receive no such coverage at all, with nothing in the build catching it; {@code
   * RequestContextFilter} is included too so query-parameter capture applies uniformly, matching
   * {@link #ICEBERG_SPEC}. Package-private and static so a unit test can exercise it directly
   * against a plain {@link JettyServer}, without booting the rest of {@link #initServer}. See
   * GH-12760.
   *
   * @param server the Jetty server whose {@link JettyServer#METRICS_PATH_SPECS} need filter
   *     coverage
   * @param eventBus the event bus audit events are dispatched through
   */
  static void registerMetricsPathFilters(JettyServer server, EventBus eventBus) {
    for (String pathSpec : JettyServer.METRICS_PATH_SPECS) {
      server.addFilter(new RequestContextFilter(eventBus), pathSpec);
      server.addFilter(
          new HttpAuditFilter(eventBus, EventSource.GRAVITINO_ICEBERG_REST_SERVER), pathSpec);
    }
  }

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties, boolean auxMode) {
    this.auxMode = auxMode;
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    initServer(icebergConfig);
    LOG.info("Iceberg REST service init. Running in {} mode", auxMode ? "auxiliary" : "standalone");
  }

  @Override
  public void serviceStart() {
    icebergMetricsManager.start();
    cleanupManager.ifPresent(IcebergCleanupManager::start);
    if (server != null) {
      try {
        server.start();
        LOG.info("Iceberg REST service started");
      } catch (Exception e) {
        // Stop the components we already started so they don't outlive a failed startup.
        cleanupManager.ifPresent(IcebergCleanupManager::close);
        icebergMetricsManager.close();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Iceberg REST service stopped");
    }
    if (configProvider != null) {
      configProvider.close();
    }
    if (icebergCatalogWrapperManager != null) {
      icebergCatalogWrapperManager.close();
    }
    if (icebergMetricsManager != null) {
      icebergMetricsManager.close();
    }
    cleanupManager.ifPresent(IcebergCleanupManager::close);
  }

  public void join() {
    if (server != null) {
      server.join();
    }
  }

  private String[] getIcebergRESTPackages(IcebergConfig icebergConfig) {
    List<String> packages = Lists.newArrayList(ICEBERG_REST_SPEC_PACKAGE);
    packages.addAll(icebergConfig.get(IcebergConfig.REST_API_EXTENSION_PACKAGES));
    return packages.toArray(new String[0]);
  }
}
