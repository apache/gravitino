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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.servlet.Filter;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.rest.RESTUtils;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestJettyServer {

  // The error page always names the servlet, so match a stack frame rather than the class name.
  private static final String FAILING_SERVLET_STACK_FRAME =
      FailingServlet.class.getName() + ".doGet(";

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
  public void testInitialize() throws IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);

    // TODO might be nice to have an isInitialised method or similar?
  }

  @Test
  public void testStartAndStop() throws RuntimeException, IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();
    // TODO might be nice to have an IsRunning method or similar?
    jettyServer.stop();
  }

  @Test
  public void testAddServletAndFilter() throws RuntimeException, IOException {
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
  public void testErrorPageIncludesStackTraceByDefault() throws IOException {
    String errorPage = requestFailingServlet(new Config(false) {});

    assertTrue(errorPage.contains(FAILING_SERVLET_STACK_FRAME), errorPage);
  }

  @Test
  public void testErrorPageOmitsStackTraceWhenDisabled() throws IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.INCLUDE_ERROR_STACK_TRACE, false);

    String errorPage = requestFailingServlet(config);

    assertFalse(errorPage.contains(FAILING_SERVLET_STACK_FRAME), errorPage);
  }

  @Test
  public void testStopWithNullServer() {
    assertDoesNotThrow(() -> jettyServer.stop());
  }

  @Test
  public void testStartWithoutInitialise() throws InterruptedException {
    assertThrows(RuntimeException.class, () -> jettyServer.start());
  }
  /** Jetty worker failures update health before logging the uncaught error. */
  @Test
  public void testUncaughtOutOfMemoryUpdatesHealth() throws IOException {
    ServerHealth health = new ServerHealth();
    Config config = new Config(false) {};
    jettyServer.initialize(JettyServerConfig.fromConfig(config), "test", false);
    Thread worker = ((QueuedThreadPool) jettyServer.getThreadPool()).newThread(() -> {});
    try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
      state.when(ServerHealth::getInstance).thenReturn(health);
      worker
          .getUncaughtExceptionHandler()
          .uncaughtException(worker, new OutOfMemoryError("Metaspace"));
      assertTrue(health.hasOutOfMemoryError());
    }
  }

  /** Starts the server with a servlet that throws, and returns Jetty's error page for it. */
  private String requestFailingServlet(Config config) throws IOException {
    int port = RESTUtils.findAvailablePort(5000, 6000);
    config.set(JettyServerConfig.WEBSERVER_HOST, "127.0.0.1");
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);
    jettyServer.initialize(JettyServerConfig.fromConfig(config), "test", false);
    jettyServer.addServlet(new FailingServlet(), "/fail");
    jettyServer.start();

    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + port + "/fail").openConnection();
    try {
      assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, connection.getResponseCode());
      try (Reader errorBody =
          new InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(errorBody);
      }
    } finally {
      connection.disconnect();
    }
  }

  private static class FailingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
      throw new IllegalStateException("servlet failure");
    }
  }
}
