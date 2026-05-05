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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.local.IdpUserManager;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.credential.CredentialOperationDispatcher;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.dto.responses.IdpUserResponse;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.lineage.LineageService;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.rest.IdpUserOperations;
import org.apache.gravitino.stats.StatisticDispatcher;
import org.apache.gravitino.tag.TagDispatcher;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.test.JerseyTest;
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
  public void testInitializeRestApiWithBasicAuthenticator() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(ImmutableMap.of(Configs.AUTHENTICATORS.getKey(), "basic"), t -> true);

    GravitinoServer restServer = newRestApiTestServer(serverConfig, Collections.emptySet());
    invokeInitializeRestApi(restServer);

    IdpUserManager userManager = Mockito.mock(IdpUserManager.class);
    Mockito.when(userManager.getUser("user1"))
        .thenReturn(
            IdpUserDTO.builder()
                .withName("user1")
                .withGroups(Collections.emptyList())
                .withAudit(buildAudit())
                .build());

    restServer.register(newIdpUserOperations(userManager));
    restServer.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(Mockito.mock(HttpServletRequest.class)).to(HttpServletRequest.class);
          }
        });

    JerseyTest jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            return restServer;
          }
        };

    try {
      jerseyTest.setUp();
      Response response =
          jerseyTest.target("/idp/users/user1").request("application/vnd.gravitino.v1+json").get();
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

      IdpUserResponse userResponse = response.readEntity(IdpUserResponse.class);
      assertEquals("user1", userResponse.getUser().name());
      assertEquals("admin", userResponse.getUser().auditInfo().creator());
    } finally {
      jerseyTest.tearDown();
    }
  }

  @Test
  public void testInitializeRestApiWithoutBasicAuthenticator() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(
        ImmutableMap.of(
            Configs.AUTHENTICATORS.getKey(), "simple",
            Configs.REST_API_EXTENSION_PACKAGES.getKey(), "org.apache.gravitino.test.extension"),
        t -> true);

    GravitinoServer restServer =
        newRestApiTestServer(serverConfig, Set.of("org.apache.gravitino.test.lineage"));

    invokeInitializeRestApi(restServer);

    JerseyTest jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            return restServer;
          }
        };

    try {
      jerseyTest.setUp();
      assertIdpInterfaceNotFound(jerseyTest, "/idp/users/user1");
      assertIdpInterfaceNotFound(jerseyTest, "/idp/groups/group1");
    } finally {
      jerseyTest.tearDown();
    }
  }

  private GravitinoServer newRestApiTestServer(
      ServerConfig serverConfig, Set<String> lineagePackages) throws IllegalAccessException {
    GravitinoEnv gravitinoEnv = Mockito.mock(GravitinoEnv.class);
    mockDispatchers(gravitinoEnv);

    GravitinoServer restServer = new GravitinoServer(serverConfig, gravitinoEnv);
    JettyServer jettyServer = Mockito.mock(JettyServer.class);
    ThreadPool threadPool = Mockito.mock(ThreadPool.class);
    LineageService lineageService = Mockito.mock(LineageService.class);

    Mockito.when(jettyServer.getThreadPool()).thenReturn(threadPool);
    Mockito.when(lineageService.getRESTPackages()).thenReturn(lineagePackages);

    FieldUtils.writeField(restServer, "server", jettyServer, true);
    FieldUtils.writeField(restServer, "lineageService", lineageService, true);
    return restServer;
  }

  private void mockDispatchers(GravitinoEnv gravitinoEnv) {
    Mockito.when(gravitinoEnv.metalakeDispatcher())
        .thenReturn(Mockito.mock(MetalakeDispatcher.class));
    Mockito.when(gravitinoEnv.catalogDispatcher())
        .thenReturn(Mockito.mock(CatalogDispatcher.class));
    Mockito.when(gravitinoEnv.schemaDispatcher()).thenReturn(Mockito.mock(SchemaDispatcher.class));
    Mockito.when(gravitinoEnv.tableDispatcher()).thenReturn(Mockito.mock(TableDispatcher.class));
    Mockito.when(gravitinoEnv.partitionDispatcher())
        .thenReturn(Mockito.mock(PartitionDispatcher.class));
    Mockito.when(gravitinoEnv.filesetDispatcher())
        .thenReturn(Mockito.mock(FilesetDispatcher.class));
    Mockito.when(gravitinoEnv.topicDispatcher()).thenReturn(Mockito.mock(TopicDispatcher.class));
    Mockito.when(gravitinoEnv.tagDispatcher()).thenReturn(Mockito.mock(TagDispatcher.class));
    Mockito.when(gravitinoEnv.policyDispatcher()).thenReturn(Mockito.mock(PolicyDispatcher.class));
    Mockito.when(gravitinoEnv.credentialOperationDispatcher())
        .thenReturn(Mockito.mock(CredentialOperationDispatcher.class));
    Mockito.when(gravitinoEnv.modelDispatcher()).thenReturn(Mockito.mock(ModelDispatcher.class));
    Mockito.when(gravitinoEnv.functionDispatcher())
        .thenReturn(Mockito.mock(FunctionDispatcher.class));
    Mockito.when(gravitinoEnv.jobOperationDispatcher())
        .thenReturn(Mockito.mock(JobOperationDispatcher.class));
    Mockito.when(gravitinoEnv.statisticDispatcher())
        .thenReturn(Mockito.mock(StatisticDispatcher.class));
  }

  private void invokeInitializeRestApi(GravitinoServer restServer) throws Exception {
    Method method = GravitinoServer.class.getDeclaredMethod("initializeRestApi");
    method.setAccessible(true);
    MetricsSystem originalMetricsSystem =
        (MetricsSystem) FieldUtils.readField(GravitinoEnv.getInstance(), "metricsSystem", true);
    try {
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "metricsSystem", Mockito.mock(MetricsSystem.class), true);
      method.invoke(restServer);
    } finally {
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "metricsSystem", originalMetricsSystem, true);
    }
  }

  private IdpUserOperations newIdpUserOperations(IdpUserManager userManager) throws Exception {
    Constructor<IdpUserOperations> constructor =
        IdpUserOperations.class.getDeclaredConstructor(IdpUserManager.class);
    constructor.setAccessible(true);
    return constructor.newInstance(userManager);
  }

  private AuditDTO buildAudit() {
    return AuditDTO.builder()
        .withCreator("admin")
        .withCreateTime(Instant.parse("2024-01-01T00:00:00Z"))
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.parse("2024-01-01T00:00:00Z"))
        .build();
  }

  private void assertIdpInterfaceNotFound(JerseyTest jerseyTest, String path) {
    Response response = jerseyTest.target(path).request("application/vnd.gravitino.v1+json").get();
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
