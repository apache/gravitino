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
package org.apache.gravitino.catalog.jdbc;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.SqliteDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the create-time backend probe and the credential-safe host extraction in {@link
 * JdbcCatalogOperations}, exercised without a real database.
 */
public class TestJdbcCatalogOperationsProbe {

  /**
   * A probe against a server that accepts the TCP connection but never replies must fail within the
   * configured read timeout, not hang. This is the metalake-write-lock-starvation case: before the
   * read timeout was injected on the probe, the driver's handshake read waited forever.
   */
  @Test
  public void testProbeAgainstAcceptButSilentServerIsBounded() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      server.setSoTimeout(0);
      // Accept and then stay silent on a background thread, mimicking a dead-backend load balancer.
      Thread acceptor =
          new Thread(
              () -> {
                try {
                  Socket socket = server.accept();
                  // Hold the socket open without ever writing a protocol handshake.
                  Thread.sleep(60_000L);
                  socket.close();
                } catch (Exception ignored) {
                  // Test is finishing; ignore.
                }
              });
      acceptor.setDaemon(true);
      acceptor.start();

      int port = server.getLocalPort();
      // Driver string is a MySQL family name so JdbcConnectionTimeouts injects connectTimeout +
      // socketTimeout (ms); the actual driver is the socket-reading fake below.
      JdbcCatalogOperations ops = new ProbeOnlyJdbcCatalogOperations(new SocketReadingDriver());
      java.util.Map<String, String> config = new java.util.HashMap<>();
      config.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
      config.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:faketimeout://127.0.0.1:" + port + "/db");
      config.put(JdbcConfig.USERNAME.getKey(), "u");
      config.put(JdbcConfig.PASSWORD.getKey(), "p");
      // 1 second probe budget; the test asserts the probe returns well within that order.
      config.put(JdbcConfig.CONNECT_TIMEOUT_MS.getKey(), "1000");
      ops.setProbeConfigForTest(new JdbcConfig(config));

      // The probe must fail fast (bounded by the injected socketTimeout), not hang on the read.
      Assertions.assertTimeoutPreemptively(
          java.time.Duration.ofSeconds(15),
          () ->
              Assertions.assertThrows(
                  ConnectionFailedException.class,
                  () -> ops.probeConnection(NameIdentifier.of("m", "c"))));
    }
  }

  /**
   * A driver that lives only in the catalog's libs directory is invisible to {@link DriverManager}
   * until its class is initialized, and the lazy pool has not done that yet when the create-time
   * probe runs. The probe must still find it instead of failing with "No suitable driver".
   */
  @Test
  public void testProbeFindsDriverVisibleOnlyToCatalogClassLoader() throws Exception {
    List<URL> urls = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      urls.add(new File(entry).toURI().toURL());
    }
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader catalogLoader =
        new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
      Thread.currentThread().setContextClassLoader(catalogLoader);
      Class<?> probeClass = Class.forName(IsolatedProbe.class.getName(), true, catalogLoader);
      Assertions.assertNotSame(IsolatedProbe.class, probeClass);

      @SuppressWarnings("unchecked")
      Callable<String> probe = (Callable<String>) probeClass.getDeclaredConstructor().newInstance();
      String message = probe.call();

      Assertions.assertTrue(message.contains(SelfRegisteringDriver.REACHED), message);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  public void testHostPortFromJdbcUrlNeverLeaksCredentials() {
    // Plain host[:port] forms are returned.
    Assertions.assertEquals(
        "db:3306", JdbcCatalogOperations.hostPortFromJdbcUrl("jdbc:mysql://db:3306/test"));
    Assertions.assertEquals(
        "db", JdbcCatalogOperations.hostPortFromJdbcUrl("jdbc:mysql://db/test"));
    Assertions.assertEquals(
        "db:3306",
        JdbcCatalogOperations.hostPortFromJdbcUrl("jdbc:mysql://root:s3cret@db:3306/test"));
    Assertions.assertEquals(
        "[::1]:5432",
        JdbcCatalogOperations.hostPortFromJdbcUrl("jdbc:postgresql://[::1]:5432/test"));

    // Credential-bearing or non-plain authorities must be reported as unknown, never echoed.
    Assertions.assertEquals(
        "unknown",
        JdbcCatalogOperations.hostPortFromJdbcUrl(
            "jdbc:mysql://(host=db1,port=3306,user=root,password=s3cret)/test"));
    Assertions.assertEquals(
        "unknown",
        JdbcCatalogOperations.hostPortFromJdbcUrl(
            "jdbc:mysql://address=(host=db1)(port=3306)(password=s3cret)/test"));
    // No "://" authority (the PostgreSQL short form can carry a password query parameter).
    Assertions.assertEquals(
        "unknown", JdbcCatalogOperations.hostPortFromJdbcUrl("jdbc:postgresql:db?password=s3cret"));
    Assertions.assertEquals("unknown", JdbcCatalogOperations.hostPortFromJdbcUrl(""));
    Assertions.assertEquals("unknown", JdbcCatalogOperations.hostPortFromJdbcUrl(null));
  }

  /** A JdbcCatalogOperations that only needs enough wiring to run the probe in a unit test. */
  private static class ProbeOnlyJdbcCatalogOperations extends JdbcCatalogOperations {
    private final Driver driver;

    ProbeOnlyJdbcCatalogOperations(Driver driver) {
      super(
          new SqliteExceptionConverter(),
          new SqliteTypeConverter(),
          new SqliteDatabaseOperations(":memory:"),
          new SqliteTableOperations(),
          new SqliteColumnDefaultValueConverter());
      this.driver = driver;
    }

    @Override
    protected Driver getDriver() {
      return driver;
    }
  }

  /**
   * A fake driver that performs a real blocking socket read against the URL's host:port, honoring a
   * {@code socketTimeout} connection property (milliseconds) exactly as a real driver would. With
   * the property set the read is bounded and surfaces as a SQLException; without it the read
   * blocks.
   */
  private static class SocketReadingDriver implements Driver {
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      String hostPort = url.substring("jdbc:faketimeout://".length());
      String host = hostPort.substring(0, hostPort.indexOf(':'));
      String rest = hostPort.substring(hostPort.indexOf(':') + 1);
      int port = Integer.parseInt(rest.substring(0, rest.indexOf('/')));
      int socketTimeoutMs =
          info.getProperty("socketTimeout") == null
              ? 0
              : Integer.parseInt(info.getProperty("socketTimeout"));
      try (Socket socket = new Socket(host, port)) {
        socket.setSoTimeout(socketTimeoutMs);
        // Mimic a protocol handshake read; a silent server makes this block until socketTimeout.
        socket.getInputStream().read();
      } catch (IOException e) {
        throw new SQLException("handshake read failed: " + e.getMessage(), e);
      }
      throw new SQLException("unexpected: silent server returned data");
    }

    @Override
    public boolean acceptsURL(String url) {
      return url != null && url.startsWith("jdbc:faketimeout://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      // This fake driver does not use java.util.logging. The FQN is required because spotless
      // rewrites every Logger import to org.slf4j.Logger.
      throw new SQLFeatureNotSupportedException();
    }
  }

  /**
   * Runs the probe from inside the catalog class loader, the way a catalog does in the server, and
   * returns the connection-failure message.
   */
  public static class IsolatedProbe implements Callable<String> {
    @Override
    public String call() {
      JdbcCatalogOperations ops =
          new JdbcCatalogOperations(
              new SqliteExceptionConverter(),
              new SqliteTypeConverter(),
              new SqliteDatabaseOperations(":memory:"),
              new SqliteTableOperations(),
              new SqliteColumnDefaultValueConverter());
      Map<String, String> config = new HashMap<>();
      config.put(JdbcConfig.JDBC_DRIVER.getKey(), SelfRegisteringDriver.class.getName());
      config.put(JdbcConfig.JDBC_URL.getKey(), SelfRegisteringDriver.URL_PREFIX + "//db:1/test");
      config.put(JdbcConfig.USERNAME.getKey(), "u");
      config.put(JdbcConfig.PASSWORD.getKey(), "p");
      ops.setProbeConfigForTest(new JdbcConfig(config));
      ConnectionFailedException e =
          Assertions.assertThrows(
              ConnectionFailedException.class,
              () -> ops.probeConnection(NameIdentifier.of("m", "c")));
      return e.getMessage();
    }
  }

  /**
   * A driver that, like real JDBC drivers, registers itself with {@link DriverManager} when its
   * class is initialized. It has no {@code META-INF/services/java.sql.Driver} entry, so nothing
   * registers it automatically.
   */
  public static class SelfRegisteringDriver implements Driver {
    static final String URL_PREFIX = "jdbc:selfregistering:";
    static final String REACHED = "reached the catalog driver";

    static {
      try {
        DriverManager.registerDriver(new SelfRegisteringDriver());
      } catch (SQLException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) {
        return null;
      }
      throw new SQLException(REACHED);
    }

    @Override
    public boolean acceptsURL(String url) {
      return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      // This fake driver does not use java.util.logging. The FQN is required because spotless
      // rewrites every Logger import to org.slf4j.Logger.
      throw new SQLFeatureNotSupportedException();
    }
  }
}
