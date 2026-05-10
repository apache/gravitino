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
package org.apache.gravitino;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.authorization.IdpManager;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestGravitinoEnv {

  private GravitinoEnv env;
  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException, IllegalAccessException {
    env = GravitinoEnv.getInstance();
    env.shutdown();
    FieldUtils.writeField(env, "idpManager", null, true);
    tempDir = Files.createTempDirectory("gravitino-env-test");
  }

  @AfterEach
  void tearDown() throws IOException {
    env.shutdown();
    FileUtils.deleteDirectory(tempDir.toFile());
  }

  @Test
  void testIdpManagerRequireInitialization() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> env.idpManager());
  }

  @Test
  void testInitializeFullComponentsLoadsIdpManager() {
    Config config = new Config(false) {};
    config.loadFromMap(
        ImmutableMap.of(
            Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
            String.format(
                "jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", tempDir.resolve("testdb")),
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            "",
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "false"),
        t -> true);

    IdpManager idpManager = Mockito.mock(IdpManager.class);
    try (MockedStatic<IdpManagerFactory> mockedFactory =
        Mockito.mockStatic(IdpManagerFactory.class)) {
      mockedFactory.when(IdpManagerFactory::createIdpManager).thenReturn(idpManager);
      env.initializeFullComponents(config);
    }

    Assertions.assertSame(idpManager, env.idpManager());
  }

  @Test
  void testShutdownClosesIdpManager() throws IllegalAccessException {
    IdpManager idpManager = Mockito.mock(IdpManager.class);
    FieldUtils.writeField(env, "idpManager", idpManager, true);

    env.shutdown();

    Mockito.verify(idpManager).close();
    Assertions.assertSame(idpManager, env.idpManager());
  }
}
