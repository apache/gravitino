/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.util;

import io.trino.testing.ResourcePresence;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockGravitinoServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MockGravitinoServer.class);

  private Server server;

  public void start(int port) throws Exception {
    server = new Server(port);
    ResourceConfig config = new ResourceConfig();
    config.packages("com.datastrato.gravitino.trino.connector.util");
    ServletHolder servlet = new ServletHolder(new ServletContainer(config));

    ServletContextHandler context = new ServletContextHandler(server, "/");
    context.addServlet(servlet, "/api/*");
    server.setHandler(context);
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }

  public int getLocalPort() throws Exception {
    if (server.getConnectors().length == 0)
      throw new Exception("MockGravitinoServer port is not initialized");
    NetworkConnector connector = (NetworkConnector) server.getConnectors()[0];
    return connector.getLocalPort();
  }

  @ResourcePresence
  public boolean isRunning() {
    return server.isRunning();
  }

  @Override
  public void close() throws Exception {
    stop();
  }
}
