/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import com.datastrato.graviton.GravitonEnv;
import com.datastrato.graviton.catalog.CatalogManager;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.meta.MetalakeManager;
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

  private final GravitonEnv gravitonEnv;

  public GravitonServer() {
    serverConfig = new ServerConfig();
    server = new JettyServer();
    gravitonEnv = GravitonEnv.getInstance();
  }

  public void initialize() {
    try {
      serverConfig.loadFromFile(CONF_FILE);
    } catch (Exception exception) {
      LOG.warn(
          "Failed to load conf from file {}, using default conf instead", CONF_FILE, exception);
    }

    server.initialize(serverConfig);

    gravitonEnv.initialize(serverConfig);

    // initialize Jersey REST API resources.
    initializeRestApi();
  }

  private void initializeRestApi() {
    packages("com.datastrato.graviton.server.web.rest");
    register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(gravitonEnv.metalakesManager()).to(MetalakeManager.class).ranked(1);
            bind(gravitonEnv.catalogManager()).to(CatalogManager.class).ranked(1);
            bind(gravitonEnv.catalogOperationDispatcher())
                .to(CatalogOperationDispatcher.class)
                .ranked(1);
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
    gravitonEnv.shutdown();
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Starting Graviton Server");
    GravitonServer server = new GravitonServer();
    server.initialize();

    try {
      // Instantiates GravitonServer
      server.start();
    } catch (Exception e) {
      LOG.error("Error while running jettyServer", e);
      System.exit(-1);
    }
    LOG.info("Done, Graviton server started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down Graviton Server ... ");
                  try {
                    server.stop();
                    Thread.sleep(3000);
                  } catch (InterruptedException e) {
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while stopping servlet container", e);
                  }
                  LOG.info("Graviton Server has shut down.");
                }));

    server.join();
  }
}
