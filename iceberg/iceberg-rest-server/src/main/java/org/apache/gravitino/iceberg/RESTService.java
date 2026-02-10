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
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;
import javax.servlet.Servlet;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceHookDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableHookDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationExecutor;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewEventDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationDispatcher;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationExecutor;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProviderFactory;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.server.web.HttpServerMetricsSource;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
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
  private IcebergConfigProvider configProvider;

  private void initServer(IcebergConfig icebergConfig) {
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(icebergConfig);
    server = new JettyServer();
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

    Boolean enableAuth = GravitinoEnv.getInstance().config().get(Configs.ENABLE_AUTHORIZATION);
    IcebergRESTServerContext authorizationContext =
        IcebergRESTServerContext.create(configProvider, enableAuth);

    EventBus eventBus = GravitinoEnv.getInstance().eventBus();
    this.icebergCatalogWrapperManager =
        new IcebergCatalogWrapperManager(configProperties, configProvider);
    this.icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    IcebergTableOperationDispatcher icebergTableOperationDispatcher =
        new IcebergTableOperationExecutor(icebergCatalogWrapperManager);
    if (authorizationContext.isAuthorizationEnabled()) {
      icebergTableOperationDispatcher =
          new IcebergTableHookDispatcher(icebergTableOperationDispatcher);
    }
    IcebergTableEventDispatcher icebergTableEventDispatcher =
        new IcebergTableEventDispatcher(icebergTableOperationDispatcher, eventBus, metalakeName);
    IcebergViewOperationExecutor icebergViewOperationExecutor =
        new IcebergViewOperationExecutor(icebergCatalogWrapperManager);
    IcebergViewEventDispatcher icebergViewEventDispatcher =
        new IcebergViewEventDispatcher(icebergViewOperationExecutor, eventBus, metalakeName);

    IcebergNamespaceOperationDispatcher namespaceOperationDispatcher =
        new IcebergNamespaceOperationExecutor(icebergCatalogWrapperManager);
    if (authorizationContext.isAuthorizationEnabled()) {
      namespaceOperationDispatcher =
          new IcebergNamespaceHookDispatcher(namespaceOperationDispatcher);
    }
    IcebergNamespaceEventDispatcher icebergNamespaceEventDispatcher =
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
            bind(icebergTableEventDispatcher).to(IcebergTableOperationDispatcher.class).ranked(1);
            bind(icebergViewEventDispatcher).to(IcebergViewOperationDispatcher.class).ranked(1);
            bind(icebergNamespaceEventDispatcher)
                .to(IcebergNamespaceOperationDispatcher.class)
                .ranked(1);
          }
        });

    Servlet servlet = new ServletContainer(config);
    server.addServlet(servlet, ICEBERG_SPEC);
    server.addCustomFilters(ICEBERG_SPEC);
    server.addSystemFilters(ICEBERG_SPEC);
  }

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    initServer(icebergConfig);
    LOG.info("Iceberg REST service init.");
  }

  @Override
  public void serviceStart() {
    icebergMetricsManager.start();
    if (server != null) {
      try {
        server.start();
        LOG.info("Iceberg REST service started");
      } catch (Exception e) {
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
