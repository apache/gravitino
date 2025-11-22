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

package org.apache.gravitino.storage.relational;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendTestExtension
    implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

  private static final Logger LOG = LoggerFactory.getLogger(BackendTestExtension.class);
  private static final String DOCKER_TEST_FLAG = "dockerTest";

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(BackendTestExtension.class);
  private static final String STORE_KEY = "BACKEND_MAP";

  @Override
  public void beforeAll(ExtensionContext context) {
    // Initialize a Map and store it at the Class level.
    // Key: backendType ("h2", "mysql"...), Value: RelationalBackend instance
    context.getStore(NAMESPACE).put(STORE_KEY, new ConcurrentHashMap<String, RelationalBackend>());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void afterAll(ExtensionContext context) {
    // Test class ended, close all started Backend
    ConcurrentHashMap<String, RelationalBackend> map =
        (ConcurrentHashMap<String, RelationalBackend>) context.getStore(NAMESPACE).get(STORE_KEY);

    if (map != null) {
      map.forEach(
          (type, backend) -> {
            try {
              LOG.info("Tearing down backend: {}", type);
              backend.close();
              // H2 special cleaning logic
              if ("h2".equals(type) && backend instanceof H2BackendWrapper) {
                ((H2BackendWrapper) backend).cleanFile();
              }
            } catch (Exception e) {
              LOG.error("Failed to close backend {}", type, e);
            }
          });
    }
  }

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    List<String> backendsToTest = new ArrayList<>();
    backendsToTest.add("h2"); // Always test with H2

    String dockerTest = System.getenv(DOCKER_TEST_FLAG);
    if ("true".equalsIgnoreCase(dockerTest)) {
      backendsToTest.add("mysql");
      backendsToTest.add("postgresql");
      LOG.info("Running tests with H2, MySQL, and PostgreSQL backends.");
    } else {
      LOG.info(
          "Running tests with H2 backend only. Set env var 'dockerTest=true' to include all backends.");
    }

    return backendsToTest.stream().map(BackendInvocationContext::new);
  }

  private static class BackendInvocationContext implements TestTemplateInvocationContext {
    private final String backendType;

    public BackendInvocationContext(String backendType) {
      this.backendType = backendType;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
      return String.format("[%s Backend]", backendType.toUpperCase());
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
      return Collections.singletonList(new BackendSetupCallback(backendType));
    }
  }

  private static class BackendSetupCallback implements BeforeEachCallback {
    private final String backendType;
    private final BaseIT baseIT = new BaseIT();

    public BackendSetupCallback(String backendType) {
      this.backendType = backendType;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {

      // Lazy loading: If the backend of this type has not started, start it
      RelationalBackend backend = startBackend(backendType);

      // Inject into the test instance
      Object testInstance = context.getRequiredTestInstance();
      if (testInstance instanceof TestJDBCBackend) {
        LOG.info("Injecting {} backend into test instance", backendType);
        ((TestJDBCBackend) testInstance).setBackend(backend);
        ((TestJDBCBackend) testInstance).setBackendType(backendType);
      }
    }

    private RelationalBackend startBackend(String type) throws Exception {
      LOG.info("Initializing backend resource: {}", type);
      Config config = Mockito.mock(Config.class);
      Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
          .thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS))
          .thenReturn(DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
          .thenReturn(DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_WAIT_MILLISECONDS);

      RelationalBackend backend = new JDBCBackend();
      if ("mysql".equals(type)) {
        String url = baseIT.startAndInitMySQLBackend();
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(url);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
            .thenReturn("com.mysql.cj.jdbc.Driver");
      } else if ("postgresql".equals(type)) {
        String url = baseIT.startAndInitPGBackend();
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(url);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
            .thenReturn("org.postgresql.Driver");
      } else {
        // H2 Logic
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String jdbcPath = "/tmp/gravitino_jdbc_test_h2_" + uuid;
        File dir = new File(jdbcPath);
        if (!dir.exists() && !dir.mkdirs()) throw new IOException("Create dir failed");

        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
            .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", jdbcPath));
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123456");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");

        // Wrap it with a Wrapper so that files can be deleted during the clean process
        backend = new H2BackendWrapper(config, jdbcPath);
      }

      // close the backend before initializing to make sure the singleton sqlSession
      // has been cleared.
      backend.close();
      backend.initialize(config);
      return backend;
    }
  }

  // A simple Wrapper solves the H2 file cleanup issue.
  public static class H2BackendWrapper extends JDBCBackend {
    private final String path;

    public H2BackendWrapper(Config config, String path) {
      this.path = path;
      super.initialize(config);
    }

    public void cleanFile() throws IOException {
      if (Files.exists(Paths.get(path))) {
        FileUtils.deleteDirectory(new File(path));
      }
    }
  }
}
