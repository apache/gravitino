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

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.apache.gravitino.server.authentication.AuthenticationFilter;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.JettyServerTestUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.eclipse.jetty.http.pathmap.ServletPathSpec;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletMapping;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGravitinoServer {

  // Static-asset and forwarding paths shared by both exemption sets below, each for the same
  // reason in both: WebUIFilter's static assets and HealthAliasServlet's forwarded probes need
  // no direct filter binding of their own on this path. See GH-12760.
  private static final Set<String> STATIC_AND_FORWARDING_PATHS =
      ImmutableSet.of(
          "/", // DefaultServlet / WebUIFilter: serves static UI assets, no server-side logic.
          "/ui/*", // WebUIFilter: serves static UI assets, no server-side logic.
          "/health/*", // HealthAliasServlet forwards into /api/health*, already covered via the
          "/health.html" // FORWARD dispatcher type; binding again here would double-log probes.
          );

  // Paths that legitimately have no HttpAuditFilter binding of their own. GH-12760: extending
  // this set is a deliberate, reviewed decision, not a default — everything else registered on
  // the servlet context must be covered.
  private static final Set<String> PATHS_EXEMPT_FROM_DIRECT_AUDIT_COVERAGE =
      STATIC_AND_FORWARDING_PATHS;

  // Paths that are deliberately reachable without authentication. GH-12760: extending this set
  // is a deliberate, reviewed decision, not a default — every other servlet path must be covered
  // by AuthenticationFilter. This is a distinct invariant from
  // PATHS_EXEMPT_FROM_DIRECT_AUDIT_COVERAGE above: e.g. /configs is fully audited but
  // intentionally unauthenticated, so it belongs in this set but not that one.
  private static final Set<String> KNOWN_PUBLIC_PATHS =
      ImmutableSet.<String>builder()
          .addAll(STATIC_AND_FORWARDING_PATHS)
          .add("/configs") // Intentionally public: backs the Web UI's pre-login OAuth bootstrap.
          .add("/configs/secrets/providers") // Open pending GH-12921 (tracked authz gate).
          .addAll(JettyServer.METRICS_PATH_SPECS) // Conventionally scraped without credentials.
          .build();

  private GravitinoServer gravitinoServer;
  private ServerConfig spyServerConfig;

  @BeforeAll
  void initConfig() throws IOException {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(
        ImmutableMap.of(
            GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            String.valueOf(RESTUtils.findAvailablePort(5000, 6000))),
        t -> true);

    spyServerConfig = Mockito.spy(serverConfig);

    Mockito.when(
            spyServerConfig.getConfigsWithPrefix(
                AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
  }

  @BeforeEach
  public void setUp() {
    gravitinoServer = new GravitinoServer(spyServerConfig, GravitinoEnv.getInstance());
  }

  @AfterAll
  public void clear() {
    String path = spyServerConfig.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH);
    if (path != null) {
      Path p = Paths.get(path).getParent();
      try {
        FileUtils.deleteDirectory(p.toFile());
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (gravitinoServer != null) {
      gravitinoServer.stop();
    }
  }

  @Test
  public void testInitialize() {
    gravitinoServer.initialize();
  }

  @Test
  public void testStartAndStop() throws Exception {
    gravitinoServer.initialize();
    gravitinoServer.start();
    gravitinoServer.stop();
  }

  @Test
  public void testStartWithoutInitialise() throws Exception {
    assertThrows(RuntimeException.class, () -> gravitinoServer.start());
  }

  @Test
  public void testStopBeforeStart() throws Exception {
    gravitinoServer.stop();
  }

  @Test
  public void testInitializeWithLoadFromFileException() throws Exception {
    ServerConfig config = new ServerConfig();

    // TODO: Exception due to environment variable not set. Is this the right exception?
    assertThrows(IllegalArgumentException.class, () -> config.loadFromFile("config"));
  }

  @Test
  public void testMainShutdownHookShouldInvokeServerStop() throws IOException {
    Path sourceFile = Path.of("src/main/java/org/apache/gravitino/server/GravitinoServer.java");
    String source = Files.readString(sourceFile);

    int hookStart = source.indexOf("addShutdownHook");
    int joinIndex = source.indexOf("server.join();", hookStart);

    assertTrue(hookStart >= 0, "Main should register a shutdown hook");
    assertTrue(
        joinIndex > hookStart, "Main should call server.join() after registering shutdown hook");

    String hookBlock = source.substring(hookStart, joinIndex);
    assertTrue(
        hookBlock.contains("server.gracefulStop()"),
        "Shutdown hook should invoke server.gracefulStop() so app-level cleanup runs on SIGTERM");
  }

  @Test
  public void testSecretProvidersDiscoveryEmpty() throws Exception {
    gravitinoServer.initialize();
    gravitinoServer.start();

    List<Map<String, Object>> providers = fetchSecretProviders(spyServerConfig);
    assertTrue(providers.isEmpty());
  }

  @Test
  public void testSecretProvidersDiscoveryWithMemoryProvider() throws Exception {
    ServerConfig serverConfig = spyServerConfig(serverConfigWithMemoryProvider());
    gravitinoServer = new GravitinoServer(serverConfig, GravitinoEnv.getInstance());
    gravitinoServer.initialize();
    gravitinoServer.start();

    List<Map<String, Object>> providers = fetchSecretProviders(serverConfig);
    assertEquals(1, providers.size());
    assertEquals("memory", providers.get(0).get("name"));
    assertEquals("memory", providers.get(0).get("type"));
    assertEquals("https://secrets.example.com", providers.get(0).get("uri"));
    assertFalse(providers.get(0).containsKey("className"));
  }

  @Test
  public void testEveryServletPathIsCoveredByAuditFilter() throws Exception {
    gravitinoServer.initialize();

    ServletHandler servletHandler = getServletContextHandler(gravitinoServer).getServletHandler();
    Set<String> auditedPathSpecs =
        JettyServerTestUtils.filterPathSpecsFor(servletHandler, HttpAuditFilter.class);

    for (ServletMapping servletMapping : servletHandler.getServletMappings()) {
      for (String pathSpec : servletMapping.getPathSpecs()) {
        if (PATHS_EXEMPT_FROM_DIRECT_AUDIT_COVERAGE.contains(pathSpec)) {
          continue;
        }
        assertTrue(
            isPathSpecCovered(pathSpec, auditedPathSpecs),
            "Servlet path '"
                + pathSpec
                + "' is registered without HttpAuditFilter coverage. See GH-12760: every "
                + "servlet mounted outside /api/* must be wired into GravitinoServer's "
                + "ROOT_MOUNTED_PATHS filter loop, or added to "
                + "PATHS_EXEMPT_FROM_DIRECT_AUDIT_COVERAGE above with a documented reason.");
      }
    }
  }

  @Test
  public void testEveryServletPathIsEitherAuthenticatedOrDeliberatelyPublic() throws Exception {
    gravitinoServer.initialize();

    ServletHandler servletHandler = getServletContextHandler(gravitinoServer).getServletHandler();
    Set<String> authenticatedPathSpecs =
        JettyServerTestUtils.filterPathSpecsFor(servletHandler, AuthenticationFilter.class);

    for (ServletMapping servletMapping : servletHandler.getServletMappings()) {
      for (String pathSpec : servletMapping.getPathSpecs()) {
        if (isPathSpecCovered(pathSpec, authenticatedPathSpecs)) {
          continue;
        }
        assertTrue(
            KNOWN_PUBLIC_PATHS.contains(pathSpec),
            "Servlet path '"
                + pathSpec
                + "' is neither covered by AuthenticationFilter nor listed in "
                + "KNOWN_PUBLIC_PATHS. See GH-12760: a servlet must either require "
                + "authentication or be a deliberate, reviewed public exception.");
      }
    }
  }

  /**
   * Whether every request a servlet registered under {@code servletPathSpec} can receive is also
   * matched by at least one of {@code filterPathSpecs}, using Jetty's own path-spec matching
   * semantics rather than exact string equality — so a servlet mounted at, say, {@code
   * /api/internal/*} is correctly recognized as already covered by a filter bound to {@code
   * /api/*}.
   *
   * @param servletPathSpec the servlet's registered pathSpec
   * @param filterPathSpecs the pathSpecs a filter is bound to
   * @return true if {@code servletPathSpec} is covered by one of {@code filterPathSpecs}
   */
  private static boolean isPathSpecCovered(String servletPathSpec, Set<String> filterPathSpecs) {
    // A path-prefix spec like "/api/*" matches any concrete path under it; substitute a
    // representative concrete path so ServletPathSpec#matches can evaluate a real request path
    // instead of a pattern.
    String representativePath =
        servletPathSpec.endsWith("/*")
            ? servletPathSpec.substring(0, servletPathSpec.length() - 1) + "probe"
            : servletPathSpec;
    return filterPathSpecs.stream()
        .anyMatch(
            filterPathSpec -> new ServletPathSpec(filterPathSpec).matches(representativePath));
  }

  private static ServletContextHandler getServletContextHandler(GravitinoServer gravitinoServer)
      throws Exception {
    Field serverField = GravitinoServer.class.getDeclaredField("server");
    serverField.setAccessible(true);
    JettyServer jettyServer = (JettyServer) serverField.get(gravitinoServer);
    return JettyServerTestUtils.getServletContextHandler(jettyServer);
  }

  private static ServerConfig serverConfigWithMemoryProvider() throws IOException {
    Map<String, String> configs = new HashMap<>();
    configs.put(
        GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(RESTUtils.findAvailablePort(5000, 6000)));
    configs.put(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    configs.put(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    configs.put(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.URI,
        "https://secrets.example.com");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(configs, t -> true);
    return serverConfig;
  }

  private static ServerConfig spyServerConfig(ServerConfig serverConfig) {
    ServerConfig spy = Mockito.spy(serverConfig);
    Mockito.when(spy.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
    return spy;
  }

  private static List<Map<String, Object>> fetchSecretProviders(ServerConfig serverConfig)
      throws Exception {
    int port =
        JettyServerConfig.fromConfig(serverConfig, GravitinoServer.WEBSERVER_CONF_PREFIX)
            .getHttpPort();
    HttpResponse<String> response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/configs/secrets/providers"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    Map<String, Object> body =
        ObjectMapperProvider.objectMapper()
            .readValue(response.body(), new TypeReference<Map<String, Object>>() {});
    return ObjectMapperProvider.objectMapper()
        .convertValue(body.get("providers"), new TypeReference<List<Map<String, Object>>>() {});
  }
}
