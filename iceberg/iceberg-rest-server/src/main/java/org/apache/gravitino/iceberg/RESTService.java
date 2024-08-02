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

import java.util.Map;
import javax.servlet.Servlet;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergTableOpsManager;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.server.web.HttpServerMetricsSource;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
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

  private IcebergTableOpsManager icebergTableOpsManager;
  private IcebergMetricsManager icebergMetricsManager;

  private void initServer(IcebergConfig icebergConfig) {
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(icebergConfig);
    server = new JettyServer();
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    server.initialize(serverConfig, SERVICE_NAME, false /* shouldEnableUI */);

    ResourceConfig config = new ResourceConfig();
    config.packages("org.apache.gravitino.iceberg.service.rest");

    config.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    config.register(IcebergExceptionMapper.class);
    HttpServerMetricsSource httpServerMetricsSource =
        new HttpServerMetricsSource(MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME, config, server);
    metricsSystem.register(httpServerMetricsSource);

    icebergTableOpsManager = new IcebergTableOpsManager(icebergConfig.getAllConfig());
    icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(icebergTableOpsManager).to(IcebergTableOpsManager.class).ranked(1);
            bind(icebergMetricsManager).to(IcebergMetricsManager.class).ranked(1);
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
    if (icebergTableOpsManager != null) {
      icebergTableOpsManager.close();
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
}
