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
package org.apache.gravitino.server;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import java.util.Properties;
import javax.servlet.Servlet;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.apache.gravitino.server.web.ConfigServlet;
import org.apache.gravitino.server.web.HttpServerMetricsSource;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.VersioningFilter;
import org.apache.gravitino.server.web.filter.AccessControlNotAllowedFilter;
import org.apache.gravitino.server.web.mapper.JsonMappingExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonParseExceptionMapper;
import org.apache.gravitino.server.web.mapper.JsonProcessingExceptionMapper;
import org.apache.gravitino.server.web.ui.WebUIFilter;
import org.apache.gravitino.tag.TagManager;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.CommonProperties;
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

  public GravitinoServer(ServerConfig config, GravitinoEnv gravitinoEnv) {
    serverConfig = config;
    server = new JettyServer();
    this.gravitinoEnv = gravitinoEnv;
  }

  public void initialize() {
    gravitinoEnv.initializeFullComponents(serverConfig);

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
    server.initialize(jettyServerConfig, SERVER_NAME, true /* shouldEnableUI */);

    ServerAuthenticator.getInstance().initialize(serverConfig);

    // initialize Jersey REST API resources.
    initializeRestApi();
  }

  public ServerConfig serverConfig() {
    return serverConfig;
  }

  private void initializeRestApi() {
    List<String> restApiPackages = Lists.newArrayList("org.apache.gravitino.server.web.rest");
    restApiPackages.addAll(serverConfig.get(Configs.REST_API_EXTENSION_PACKAGES));
    packages(restApiPackages.toArray(new String[0]));

    boolean enableAuthorization = serverConfig.get(Configs.ENABLE_AUTHORIZATION);
    register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(gravitinoEnv.metalakeDispatcher()).to(MetalakeDispatcher.class).ranked(1);
            bind(gravitinoEnv.catalogDispatcher()).to(CatalogDispatcher.class).ranked(1);
            bind(gravitinoEnv.schemaDispatcher()).to(SchemaDispatcher.class).ranked(1);
            bind(gravitinoEnv.tableDispatcher()).to(TableDispatcher.class).ranked(1);
            bind(gravitinoEnv.partitionDispatcher()).to(PartitionDispatcher.class).ranked(1);
            bind(gravitinoEnv.filesetDispatcher()).to(FilesetDispatcher.class).ranked(1);
            bind(gravitinoEnv.topicDispatcher()).to(TopicDispatcher.class).ranked(1);
            bind(gravitinoEnv.tagManager()).to(TagManager.class).ranked(1);
          }
        });
    register(JsonProcessingExceptionMapper.class);
    register(JsonParseExceptionMapper.class);
    register(JsonMappingExceptionMapper.class);
    register(ObjectMapperProvider.class).register(JacksonFeature.class);
    property(CommonProperties.JSON_JACKSON_DISABLED_MODULES, "DefaultScalaModule");

    if (!enableAuthorization) {
      register(AccessControlNotAllowedFilter.class);
    }

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
    GravitinoServer server = new GravitinoServer(serverConfig, GravitinoEnv.getInstance());
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
