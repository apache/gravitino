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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.servlet.Filter;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.utils.RequestContext;
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
  public void testMetricsEndpointsAreNotRegisteredByDefault() throws IOException {
    int port = startServer();

    for (String pathSpec : JettyServer.METRICS_PATH_SPECS) {
      assertEquals(HttpServletResponse.SC_NOT_FOUND, requestStatus(port, pathSpec));
    }
  }

  @Test
  public void testMetricsEndpointsCanBeRegisteredExplicitly() throws IOException {
    int port = RESTUtils.findAvailablePort(5000, 6000);
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HOST, "127.0.0.1");
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);
    jettyServer.initialize(JettyServerConfig.fromConfig(config), "test", false);
    try (MetricsSystem metricsSystem = new MetricsSystem()) {
      jettyServer.addMetricsServlets(metricsSystem);
      jettyServer.start();

      for (String pathSpec : JettyServer.METRICS_PATH_SPECS) {
        assertEquals(HttpServletResponse.SC_OK, requestStatus(port, pathSpec));
      }
    }
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

  /**
   * Sends real requests over IPv4 and IPv6 loopback, so the peer address comes from Jetty itself
   * (which brackets IPv6 peers) rather than from a mock.
   */
  @Test
  public void testTrustedProxyResolvesClientAddressOverRealConnections() throws IOException {
    int port = startServerWithClientAddressEcho("127.0.0.1, ::1");

    assertEquals("1.2.3.4", requestClientAddress("127.0.0.1", port, "9.9.9.9, 1.2.3.4"));
    assertEquals("127.0.0.1", requestClientAddress("127.0.0.1", port, null));
    assumeTrue(isIpv6LoopbackReachable(port), "IPv6 loopback is not available");
    assertEquals("1.2.3.4", requestClientAddress("[::1]", port, "9.9.9.9, 1.2.3.4"));
    // Jetty reports the peer as "[0:0:0:0:0:0:0:1]"; the recorded form is canonical.
    assertEquals("::1", requestClientAddress("[::1]", port, null));
  }

  @Test
  public void testForwardedForIgnoredByDefaultOverRealConnection() throws IOException {
    int port = startServerWithClientAddressEcho(null);

    assertEquals("127.0.0.1", requestClientAddress("127.0.0.1", port, "1.2.3.4"));
  }

  /**
   * Starts the server with {@link RequestContextFilter} wired to the configured resolver, as the
   * real servers do, and a servlet that echoes the resolved client address.
   */
  private int startServerWithClientAddressEcho(String trustedProxies) throws IOException {
    int port = RESTUtils.findAvailablePort(5000, 6000);
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);
    if (trustedProxies != null) {
      config.set(JettyServerConfig.TRUSTED_PROXIES, Arrays.asList(trustedProxies.split(",")));
    }
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.addServlet(new ClientAddressEchoServlet(), "/echo");
    jettyServer.addFilter(
        new RequestContextFilter(null, serverConfig.getClientAddressResolver()), "/echo");
    jettyServer.start();
    return port;
  }

  private String requestClientAddress(String host, int port, String xForwardedFor)
      throws IOException {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://" + host + ":" + port + "/echo").openConnection();
    try {
      if (xForwardedFor != null) {
        connection.setRequestProperty(ClientAddressResolver.X_FORWARDED_FOR, xForwardedFor);
      }
      assertEquals(HttpServletResponse.SC_OK, connection.getResponseCode());
      try (Reader body =
          new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(body);
      }
    } finally {
      connection.disconnect();
    }
  }

  private static boolean isIpv6LoopbackReachable(int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("::1", port), 1000);
      return true;
    } catch (IOException e) {
      return false;
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

  private int startServer() throws IOException {
    int port = RESTUtils.findAvailablePort(5000, 6000);
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HOST, "127.0.0.1");
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);
    jettyServer.initialize(JettyServerConfig.fromConfig(config), "test", false);
    jettyServer.start();
    return port;
  }

  private int requestStatus(int port, String path) throws IOException {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static class ClientAddressEchoServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      response.setContentType("text/plain");
      response.getWriter().write(RequestContext.getRemoteAddress());
    }
  }

  private static class FailingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
      throw new IllegalStateException("servlet failure");
    }
  }
}
