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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

class BackendTestExtension implements TestTemplateInvocationContextProvider {
  private static final String DOCKER_TEST_FLAG = "dockerTest";

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    List<String> backends =
        "true".equalsIgnoreCase(System.getenv(DOCKER_TEST_FLAG))
            ? List.of("h2", "mysql", "postgresql")
            : List.of("h2");
    return backends.stream().map(BackendInvocationContext::new);
  }

  private static class BackendInvocationContext implements TestTemplateInvocationContext {
    private final String backendType;

    private BackendInvocationContext(String backendType) {
      this.backendType = backendType;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
      return String.format("[%s Backend]", backendType.toUpperCase());
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
      return List.of(new BackendSetupCallback(backendType));
    }
  }

  private static class BackendSetupCallback implements BeforeEachCallback, AfterEachCallback {
    private final BaseIT baseIT = new BaseIT();
    private final String backendType;

    private JDBCBackend backend;
    private Path h2Path;

    private BackendSetupCallback(String backendType) {
      this.backendType = backendType;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      backend = startBackend();
      Object testInstance = context.getRequiredTestInstance();
      if (testInstance instanceof TestJDBCBackend) {
        ((TestJDBCBackend) testInstance).setBackendType(backendType);
        ((TestJDBCBackend) testInstance).setBackend(backend);
      }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      SqlSessionFactoryHelper.getInstance().close();
      if (backend != null) {
        backend.close();
        backend = null;
      }

      if (h2Path != null && Files.exists(h2Path)) {
        deleteDirectory(h2Path);
        h2Path = null;
      }
    }

    private JDBCBackend startBackend() throws SQLException {
      Config config = new Config(false) {};
      config.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
      config.set(Configs.ENTITY_RELATIONAL_STORE, backendType);
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 20);
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);

      if ("mysql".equals(backendType)) {
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, baseIT.startAndInitMySQLBackend());
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "root");
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "com.mysql.cj.jdbc.Driver");
      } else if ("postgresql".equals(backendType)) {
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, baseIT.startAndInitPGBackend());
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "root");
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.postgresql.Driver");
      } else {
        String jdbcStorePath =
            "/tmp/gravitino_jdbc_idpMappers_" + UUID.randomUUID().toString().replace("-", "");
        h2Path = Path.of(jdbcStorePath);
        try {
          Files.createDirectories(h2Path);
        } catch (IOException e) {
          throw new RuntimeException("Create H2 test directory failed: " + h2Path, e);
        }

        config.set(
            Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
            String.format("jdbc:h2:file:%s/testdb;DB_CLOSE_DELAY=-1;MODE=MYSQL", jdbcStorePath));
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "123456");
        config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
      }

      JDBCBackend jdbcBackend = new JDBCBackend();
      jdbcBackend.initialize(config);
      return jdbcBackend;
    }

    private void deleteDirectory(Path dir) throws IOException {
      try (Stream<Path> paths = Files.walk(dir)) {
        paths
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.deleteIfExists(path);
                  } catch (IOException e) {
                    throw new RuntimeException("Delete path failed: " + path, e);
                  }
                });
      }
    }
  }
}
