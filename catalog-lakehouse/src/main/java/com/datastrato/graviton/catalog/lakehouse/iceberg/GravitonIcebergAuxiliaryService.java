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

public class GravitonIcebergAuxiliaryService implements GravitonAuxiliaryService {

  Logger LOG = LoggerFactory.getLogger(GravitonIcebergAuxiliaryService.class);

  Server server;

  private Server initServer(Map<String, String> config) {
    Server server = new Server(9001);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.packages("com.datastrato.graviton.catalog.lakehouse.iceberg.web.rest");

    resourceConfig.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    resourceConfig.register(IcebergExceptionMapper.class);

    IcebergTableOps icebergTableOps = new IcebergTableOps(config);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(icebergTableOps).to(IcebergTableOps.class).ranked(2);
          }
        });

    ServletHolder servlet = new ServletHolder(new ServletContainer(resourceConfig));

    ServletContextHandler context = new ServletContextHandler(server, "/");
    context.addServlet(servlet, "/iceberg/*");
    return server;
  }

  @Override
  public String shortName() {
    return "GravitonIcebergREST";
  }

  @Override
  public void serviceInit(Map<String, String> config) {
    server = initServer(config);
    LOG.info("Iceberg aux service inited");
  }

  @Override
  public void serviceStart() {
    if (server != null) {
      try {
        server.start();
        LOG.info("Iceberg aux service started");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void serviceStop() {
    if (server != null) {
      try {
        server.stop();
        LOG.info("Iceberg aux service stopped");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
