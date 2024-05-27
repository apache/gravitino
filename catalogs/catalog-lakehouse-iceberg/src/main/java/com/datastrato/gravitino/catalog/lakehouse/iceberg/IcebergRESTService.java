/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.auxiliary.GravitinoAuxiliaryService;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergExceptionMapper;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics.IcebergMetricsManager;
import com.datastrato.gravitino.metrics.MetricsSystem;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import com.datastrato.gravitino.server.web.HttpServerMetricsSource;
import com.datastrato.gravitino.server.web.JettyServer;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import java.util.Map;
import javax.servlet.Servlet;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTService implements GravitinoAuxiliaryService {

  private static Logger LOG = LoggerFactory.getLogger(IcebergRESTService.class);

  private JettyServer server;

  public static final String SERVICE_NAME = "iceberg-rest";
  public static final String ICEBERG_SPEC = "/iceberg/*";

  private IcebergTableOps icebergTableOps;
  private IcebergMetricsManager icebergMetricsManager;

  private void initServer(IcebergConfig icebergConfig) {
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(icebergConfig);
    server = new JettyServer();
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    server.initialize(serverConfig, SERVICE_NAME, false /* shouldEnableUI */);

    ResourceConfig config = new ResourceConfig();
    config.packages("com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest");

    config.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    config.register(IcebergExceptionMapper.class);
    HttpServerMetricsSource httpServerMetricsSource =
        new HttpServerMetricsSource(MetricsSource.ICEBERG_REST_SERVER_METRIC_NAME, config, server);
    metricsSystem.register(httpServerMetricsSource);

    icebergTableOps = new IcebergTableOps(icebergConfig);
    icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(icebergTableOps).to(IcebergTableOps.class).ranked(1);
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
    LOG.info("Iceberg REST service inited");
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
    if (icebergTableOps != null) {
      icebergTableOps.close();
    }
    if (icebergMetricsManager != null) {
      icebergMetricsManager.close();
    }
  }
}
