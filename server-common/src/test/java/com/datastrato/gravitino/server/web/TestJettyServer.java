/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.rest.RESTUtils;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.Servlet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJettyServer {

  private JettyServer jettyServer;

  @BeforeEach
  public void setUp() {
    jettyServer = new JettyServer();
  }

  @AfterEach
  public void tearDown() {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }

  @Test
  void testInitialize() throws IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);

    // TODO might be nice to have an isInitalised method or similar?
  }

  @Test
  void testStartAndStop() throws RuntimeException, IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();
    // TODO might be nice to have an IsRunning method or similar?
    jettyServer.stop();
  }

  @Test
  void testAddServletAndFilter() throws RuntimeException, IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();

    Servlet mockServlet = mock(Servlet.class);
    Filter mockFilter = mock(Filter.class);
    jettyServer.addServlet(mockServlet, "/test");
    jettyServer.addFilter(mockFilter, "/filter");

    // TODO add asserts

    jettyServer.stop();
  }

  @Test
  void testStopWithNullServer() {
    assertDoesNotThrow(() -> jettyServer.stop());
  }

  @Test
  void testStartWithoutInitialise() throws InterruptedException {
    assertThrows(RuntimeException.class, () -> jettyServer.start());
  }
}
