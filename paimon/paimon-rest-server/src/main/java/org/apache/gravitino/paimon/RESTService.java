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
package org.apache.gravitino.paimon;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.Servlet;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.paimon.service.PaimonAuthenticationFilter;
import org.apache.gravitino.paimon.service.PaimonAuthorization;
import org.apache.gravitino.paimon.service.PaimonCatalogRequest;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager;
import org.apache.gravitino.paimon.service.PaimonExceptionMapper;
import org.apache.gravitino.paimon.service.PaimonHealthCheckPathMatcher;
import org.apache.gravitino.paimon.service.PaimonMetadataHooks;
import org.apache.gravitino.server.web.ClientAddressResolver;
import org.apache.gravitino.server.web.HealthAliasServlet;
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

/**
 * The Paimon REST catalog server exposed as a Gravitino auxiliary service. It runs its own
 * Jetty+Jersey instance (separate from the main Gravitino web server) and serves the Paimon REST
 * catalog protocol so that engines can connect with Paimon's native {@code RESTCatalog} client.
 *
 * <p>The service can run embedded in the Gravitino server process (auxiliary mode, enabled by
 * adding {@code paimon-rest} to {@code gravitino.auxService.names}) or as a standalone process via
 * {@link org.apache.gravitino.paimon.server.GravitinoPaimonRESTServer}.
 */
public class RESTService implements GravitinoAuxiliaryService {

  private static final Logger LOG = LoggerFactory.getLogger(RESTService.class);

  public static final String SERVICE_NAME = "paimon-rest";
  public static final String PAIMON_SPEC = "/paimon/*";
  private static final String PAIMON_REST_SPEC_PACKAGE = "org.apache.gravitino.paimon.service.rest";

  private JettyServer server;
  private PaimonCatalogWrapperManager catalogWrapperManager;

  private void initServer(
      PaimonConfig paimonConfig, boolean auxiliary, boolean authorizationEnabled) {
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(paimonConfig);
    server =
        new JettyServer() {
          @Override
          protected Filter createAuthenticationFilter(boolean includeErrorStackTrace) {
            return new PaimonAuthenticationFilter(
                paimonConfig.get(PaimonConfig.BEARER_TOKEN), paimonConfig.getPrincipalTokens());
          }
        };
    server.initialize(serverConfig, SERVICE_NAME, false /* shouldEnableUI */);

    ResourceConfig config = new ResourceConfig();
    config.packages(getPaimonRESTPackages(paimonConfig));
    config.register(JacksonFeature.withoutExceptionMappers());
    config.register(PaimonExceptionMapper.class);

    this.catalogWrapperManager = new PaimonCatalogWrapperManager(paimonConfig, auxiliary);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(catalogWrapperManager).to(PaimonCatalogWrapperManager.class).ranked(1);
            bind(new PaimonAuthorization(
                    authorizationEnabled,
                    paimonConfig
                        .getAllConfig()
                        .getOrDefault("gravitino-metalake", "default_metalake")))
                .to(PaimonAuthorization.class);
            bind(new PaimonMetadataHooks(
                    auxiliary
                        && "dynamic-config-provider"
                            .equals(paimonConfig.getAllConfig().get("catalog-config-provider")),
                    paimonConfig
                        .getAllConfig()
                        .getOrDefault("gravitino-metalake", "default_metalake")))
                .to(PaimonMetadataHooks.class);
            bindAsContract(PaimonCatalogRequest.class);
          }
        });

    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      HttpServerMetricsSource httpServerMetricsSource =
          new HttpServerMetricsSource(MetricsSource.PAIMON_REST_SERVER_METRIC_NAME, config, server);
      metricsSystem.register(httpServerMetricsSource);
    } else {
      LOG.info("Metrics system is not initialized; skipping metrics registration");
    }

    Servlet servlet = new ServletContainer(config);
    server.addServlet(servlet, PAIMON_SPEC);
    EventBus eventBus = eventBusOrNull();
    if (eventBus != null) {
      ClientAddressResolver addressResolver = serverConfig.getClientAddressResolver();
      server.addFilter(new RequestContextFilter(eventBus, addressResolver), PAIMON_SPEC);
      server.addFilter(
          new HttpAuditFilter(
              eventBus,
              EventSource.GRAVITINO_PAIMON_REST_SERVER,
              new PaimonHealthCheckPathMatcher(),
              addressResolver),
          PAIMON_SPEC);
    } else {
      LOG.info("Event bus is not initialized; skipping audit filter registration");
    }
    server.addCustomFilters(PAIMON_SPEC);
    server.addSystemFilters(PAIMON_SPEC);

    // Root-level aliases for health checks to improve compatibility with various monitoring
    // systems that expect a /health endpoint.
    server.addServlet(new HealthAliasServlet("/paimon"), "/health/*");
    server.addServlet(new HealthAliasServlet("/paimon"), "/health.html");
  }

  @Nullable
  private EventBus eventBusOrNull() {
    try {
      return GravitinoEnv.getInstance().eventBus();
    } catch (IllegalArgumentException e) {
      // Direct tests can start the REST service without initializing Gravitino's event bus.
      return null;
    }
  }

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties, boolean auxMode) {
    boolean authorizationEnabled = false;
    if (auxMode) {
      Config config = GravitinoEnv.getInstance().config();
      Preconditions.checkState(config != null, "Gravitino must be initialized for auxiliary mode");
      authorizationEnabled = config.get(Configs.ENABLE_AUTHORIZATION);
      Preconditions.checkState(
          !authorizationEnabled
              || "dynamic-config-provider".equals(properties.get("catalog-config-provider")),
          "Gravitino authorization requires dynamic-config-provider to resolve registered catalogs.");
    }
    PaimonConfig paimonConfig = new PaimonConfig(properties);
    initServer(paimonConfig, auxMode, authorizationEnabled);
    LOG.info("Paimon REST service init. Running in {} mode", auxMode ? "auxiliary" : "standalone");
  }

  @Override
  public void serviceStart() {
    if (server != null) {
      try {
        server.start();
        LOG.info("Paimon REST service started");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Paimon REST service stopped");
    }
    if (catalogWrapperManager != null) {
      catalogWrapperManager.close();
    }
  }

  public void join() {
    if (server != null) {
      server.join();
    }
  }

  private String[] getPaimonRESTPackages(PaimonConfig paimonConfig) {
    List<String> packages = Lists.newArrayList(PAIMON_REST_SPEC_PACKAGE);
    packages.addAll(paimonConfig.get(PaimonConfig.REST_API_EXTENSION_PACKAGES));
    return packages.toArray(new String[0]);
  }
}
