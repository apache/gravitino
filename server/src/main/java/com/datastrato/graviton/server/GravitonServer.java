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
import com.datastrato.graviton.server.web.JettyServerConfig;
import com.datastrato.graviton.server.web.ObjectMapperProvider;
import com.datastrato.graviton.server.web.VersioningFilter;
import java.io.File;
import java.util.Properties;
import javax.servlet.Servlet;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonServer extends ResourceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(GravitonServer.class);

  public static final String CONF_FILE = "graviton.conf";

  public static final String WEBSERVER_CONF_PREFIX = "graviton.server.webserver.";

  public static final String SERVER_NAME = "Graviton-webserver";

  private final ServerConfig serverConfig;

  private final JettyServer server;

  private final GravitonEnv gravitonEnv;

  public GravitonServer(ServerConfig config) {
    serverConfig = config;
    server = new JettyServer();
    gravitonEnv = GravitonEnv.getInstance();
  }

  public void initialize() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
    server.initialize(jettyServerConfig, SERVER_NAME);

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
    gravitonEnv.start();
  }

  public void join() {
    server.join();
  }

  public void stop() {
    server.stop();
    gravitonEnv.shutdown();
  }

  public static void main(String[] args) {
    LOG.info("Starting Graviton Server");
    String confPath = System.getenv("GRAVITON_TEST") == null ? "" : args[0];
    ServerConfig serverConfig = loadConfig(confPath);
    GravitonServer server = new GravitonServer(serverConfig);
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
                  try {
                    // Register some clean-up tasks that need to be done before shutting down
                    Thread.sleep(server.serverConfig.get(ServerConfig.SERVER_SHUTDOWN_TIMEOUT));
                  } catch (InterruptedException e) {
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while running clean-up tasks in shutdown hook", e);
                  }
                }));

    server.join();

    LOG.info("Shutting down Graviton Server ... ");
    try {
      server.stop();
      LOG.info("Graviton Server has shut down.");
    } catch (Exception e) {
      LOG.error("Error while stopping Graviton Server", e);
    }
  }

  static ServerConfig loadConfig(String confPath) {
    ServerConfig serverConfig = new ServerConfig();
    try {
      if (confPath.isEmpty()) {
        // Load default conf
        serverConfig.loadFromFile(CONF_FILE);
      } else {
        Properties properties = serverConfig.loadPropertiesFromFile(new File(confPath));
        serverConfig.loadFromProperties(properties);
      }
    } catch (Exception exception) {
      throw new IllegalArgumentException("Failed to load conf from file " + confPath, exception);
    }
    return serverConfig;
  }
}
