/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.meta.MetalakeManager;
import com.datastrato.gravitino.metrics.MetricsSystem;
import com.datastrato.gravitino.metrics.source.MetricsSource;
import com.datastrato.gravitino.server.auth.ServerAuthenticator;
import com.datastrato.gravitino.server.web.ConfigServlet;
import com.datastrato.gravitino.server.web.HttpServerMetricsSource;
import com.datastrato.gravitino.server.web.JettyServer;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.datastrato.gravitino.server.web.ObjectMapperProvider;
import com.datastrato.gravitino.server.web.VersioningFilter;
import com.datastrato.gravitino.server.web.ui.WebUIFilter;
import java.io.File;
import java.util.Properties;
import javax.servlet.Servlet;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoServer extends ResourceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoServer.class);

  private static final String API_ANY_PATH = "/api/*";

  public static final String CONF_FILE = "gravitino.conf";

  public static final String WEBSERVER_CONF_PREFIX = "gravitino.server.webserver.";

  public static final String SERVER_NAME = "Gravitino-webserver";

  private final ServerConfig serverConfig;

  private final JettyServer server;

  private final GravitinoEnv gravitinoEnv;

  public GravitinoServer(ServerConfig config) {
    serverConfig = config;
    server = new JettyServer();
    gravitinoEnv = GravitinoEnv.getInstance();
  }

  public void initialize() {
    gravitinoEnv.initialize(serverConfig);

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
    server.initialize(jettyServerConfig, SERVER_NAME, true /* shouldEnableUI */);

    ServerAuthenticator.getInstance().initialize(serverConfig);

    // initialize Jersey REST API resources.
    initializeRestApi();
  }

  private void initializeRestApi() {
    packages("com.datastrato.gravitino.server.web.rest");
    register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(gravitinoEnv.metalakesManager()).to(MetalakeManager.class).ranked(1);
            bind(gravitinoEnv.catalogManager()).to(CatalogManager.class).ranked(1);
            bind(gravitinoEnv.catalogOperationDispatcher())
                .to(CatalogOperationDispatcher.class)
                .ranked(1);
          }
        });
    register(ObjectMapperProvider.class).register(JacksonFeature.class);

    HttpServerMetricsSource httpServerMetricsSource =
        new HttpServerMetricsSource(MetricsSource.GRAVITINO_SERVER_METRIC_NAME, this, server);
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    metricsSystem.register(httpServerMetricsSource);

    Servlet servlet = new ServletContainer(this);
    server.addServlet(servlet, API_ANY_PATH);
    Servlet configServlet = new ConfigServlet(serverConfig);
    server.addServlet(configServlet, "/configs");
    server.addCustomFilters(API_ANY_PATH);
    server.addFilter(new VersioningFilter(), API_ANY_PATH);
    server.addSystemFilters(API_ANY_PATH);
    server.addFilter(new WebUIFilter(), "/"); // Redirect to the /ui/index html page.
    server.addFilter(new WebUIFilter(), "/ui/*"); // Redirect to the static html file.
  }

  public void start() throws Exception {
    gravitinoEnv.start();
    server.start();
  }

  public void join() {
    server.join();
  }

  public void stop() {
    server.stop();
    gravitinoEnv.shutdown();
  }

  public static void main(String[] args) {
    LOG.info("Starting Gravitino Server");
    String confPath = System.getenv("GRAVITINO_TEST") == null ? "" : args[0];
    ServerConfig serverConfig = loadConfig(confPath);
    GravitinoServer server = new GravitinoServer(serverConfig);
    server.initialize();

    try {
      // Instantiates GravitinoServer
      server.start();
    } catch (Exception e) {
      LOG.error("Error while running jettyServer", e);
      System.exit(-1);
    }
    LOG.info("Done, Gravitino server started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    // Register some clean-up tasks that need to be done before shutting down
                    Thread.sleep(server.serverConfig.get(ServerConfig.SERVER_SHUTDOWN_TIMEOUT));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while running clean-up tasks in shutdown hook", e);
                  }
                }));

    server.join();

    LOG.info("Shutting down Gravitino Server ... ");
    try {
      server.stop();
      LOG.info("Gravitino Server has shut down.");
    } catch (Exception e) {
      LOG.error("Error while stopping Gravitino Server", e);
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
