/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.aux.GravitonAuxiliaryService;
import com.datastrato.graviton.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergExceptionMapper;
import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import java.util.Map;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergAuxiliaryService implements GravitonAuxiliaryService {

  Logger LOG = LoggerFactory.getLogger(IcebergAuxiliaryService.class);

  Server server;

  public static final String SERVICE_NAME = "GravitonIcebergREST";

  private Server initServer(IcebergRESTConfig restConfig) {
    // todo, use JettyServer when it's moved to graviton common package
    int port = restConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT);
    LOG.info("Iceberg REST service http port:{}", port);
    Server server = new Server(port);

    ResourceConfig config = new ResourceConfig();
    config.packages("com.datastrato.graviton.catalog.lakehouse.iceberg.web.rest");

    config.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    config.register(IcebergExceptionMapper.class);

    IcebergTableOps icebergTableOps = new IcebergTableOps(restConfig);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(icebergTableOps).to(IcebergTableOps.class).ranked(2);
          }
        });

    ServletHolder servlet = new ServletHolder(new ServletContainer(config));

    ServletContextHandler context = new ServletContextHandler(server, "/");
    context.addServlet(servlet, "/iceberg/*");
    return server;
  }

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties) {
    IcebergRESTConfig icebergConfig = new IcebergRESTConfig();
    icebergConfig.loadFromMap(properties, false);
    server = initServer(icebergConfig);
    LOG.info("Iceberg REST service inited");
  }

  @Override
  public void serviceStart() {
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
  }
}
