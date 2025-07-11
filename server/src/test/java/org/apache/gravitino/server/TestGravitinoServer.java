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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
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
}
