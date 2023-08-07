/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.datastrato.graviton.server.GravitonServerException;
import com.datastrato.graviton.server.ServerConfig;
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
  public void testInitialize() {
    ServerConfig config = new ServerConfig();

    jettyServer.initialize(config);

    // TODO might be nice to have an isInitalised method or similar?
  }

  @Test
  public void testStartAndStop() throws GravitonServerException {
    ServerConfig config = new ServerConfig();

    jettyServer.initialize(config);
    jettyServer.start();
    // TODO might be nice to have an IsRunning method or similar?
    jettyServer.stop();
  }

  @Test
  public void testAddServletAndFilter() throws GravitonServerException {
    ServerConfig config = new ServerConfig();

    jettyServer.initialize(config);
    jettyServer.start();

    Servlet mockServlet = mock(Servlet.class);
    Filter mockFilter = mock(Filter.class);
    jettyServer.addServlet(mockServlet, "/test");
    jettyServer.addFilter(mockFilter, "/filter");

    // TODO add asserts

    jettyServer.stop();
  }

  @Test
  public void testStopWithNullServer() {
    assertDoesNotThrow(() -> jettyServer.stop());
  }

  @Test
  public void testStartWithoutInitialise() throws InterruptedException {
    assertThrows(GravitonServerException.class, () -> jettyServer.start());
  }
}
