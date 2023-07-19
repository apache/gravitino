/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import com.datastrato.graviton.meta.BaseCatalogsOperations;
import com.datastrato.graviton.meta.BaseMetalakesOperations;
import com.datastrato.graviton.server.web.JettyServer;
import com.datastrato.graviton.server.web.ObjectMapperProvider;
import com.datastrato.graviton.server.web.VersioningFilter;
import javax.servlet.Servlet;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonServer extends ResourceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(GravitonServer.class);

  private static final String CONF_FILE = "graviton.conf";

  private final ServerConfig serverConfig;

  private final JettyServer server;

  public GravitonServer() {
    serverConfig = new ServerConfig();
    server = new JettyServer();
  }

  public void initialize() {
    try {
      serverConfig.loadFromFile(CONF_FILE);
    } catch (Exception exception) {
      LOG.warn(
          "Failed to load conf from file {}, using default conf instead", CONF_FILE, exception);
    }

    server.initialize(serverConfig);

    // initialize Jersey REST API resources.
    initializeRestApi();
  }

  private void initializeRestApi() {
    packages("com.datastrato.graviton.server.web.rest");
    register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(BaseMetalakesOperations.class).to(BaseMetalakesOperations.class).ranked(1);
            bind(BaseCatalogsOperations.class).to(BaseCatalogsOperations.class).ranked(1);
          }
        });
    register(ObjectMapperProvider.class).register(JacksonFeature.class);

    Servlet servlet = new ServletContainer(this);
    server.addServlet(servlet, "/api/*");
    server.addFilter(new VersioningFilter(), "/api/*");
  }

  public void start() throws Exception {
    server.start();
  }

  public void join() {
    server.join();
  }

  public void stop() {
    server.stop();
  }

  public static void main(String[] args) throws Exception {
    GravitonServer server = new GravitonServer();
    server.initialize();

    try {
      server.start();
      server.join();
    } finally {
      server.stop();
    }
  }
}
