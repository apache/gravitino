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

package org.apache.gravitino.idp.basic.storage.relational;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class BackendTestExtension
    implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

  private static final String DOCKER_TEST_FLAG = "dockerTest";
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(BackendTestExtension.class);
  private static final String STORE_KEY = "BACKEND_MAP";

  @Override
  public void beforeAll(ExtensionContext context) {
    context.getStore(NAMESPACE).put(STORE_KEY, new ConcurrentHashMap<String, BackendResource>());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void afterAll(ExtensionContext context) throws Exception {
    ConcurrentHashMap<String, BackendResource> map =
        (ConcurrentHashMap<String, BackendResource>) context.getStore(NAMESPACE).get(STORE_KEY);
    if (map != null) {
      for (BackendResource backendResource : map.values()) {
        backendResource.close();
      }
      map.clear();
    }
  }

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return BackendAware.class.isAssignableFrom(context.getRequiredTestClass());
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    return resolveBackends(context.getRequiredTestClass()).stream()
        .map(BackendInvocationContext::new);
  }

  private List<String> resolveBackends(Class<?> testClass) {
    BackendTypes backendTypes = findBackendTypes(testClass);
    if (backendTypes != null) {
      return List.of(backendTypes.value());
    }

    List<String> backendsToTest = new ArrayList<>();
    backendsToTest.add("h2");
    if ("true".equalsIgnoreCase(System.getenv(DOCKER_TEST_FLAG))) {
      backendsToTest.add("mysql");
      backendsToTest.add("postgresql");
    }
    return backendsToTest;
  }

  private BackendTypes findBackendTypes(Class<?> testClass) {
    Class<?> current = testClass;
    while (current != null) {
      BackendTypes backendTypes = current.getDeclaredAnnotation(BackendTypes.class);
      if (backendTypes != null) {
        return backendTypes;
      }
      current = current.getSuperclass();
    }
    return null;
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
      return Collections.singletonList(new BackendSetupCallback(backendType));
    }
  }

  private static class BackendSetupCallback implements BeforeEachCallback {
    private final String backendType;

    private BackendSetupCallback(String backendType) {
      this.backendType = backendType;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      BackendResource backendResource = getOrCreateBackendResource(context, backendType);
      Object testInstance = context.getRequiredTestInstance();
      if (testInstance instanceof BackendAware) {
        ((BackendAware) testInstance).setBackend(backendResource.backend());
        ((BackendAware) testInstance).setBackendType(backendType);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, BackendResource> getBackendResources(ExtensionContext context) {
    return (Map<String, BackendResource>) context.getStore(NAMESPACE).get(STORE_KEY, Map.class);
  }

  private static BackendResource getOrCreateBackendResource(
      ExtensionContext context, String backendType) throws SQLException {
    Map<String, BackendResource> backendResources = getBackendResources(context);
    synchronized (backendResources) {
      BackendResource backendResource = backendResources.get(backendType);
      if (backendResource != null) {
        return backendResource;
      }

      backendResource = createBackendResource(backendType);
      backendResources.put(backendType, backendResource);
      return backendResource;
    }
  }

  private static BackendResource createBackendResource(String backendType) throws SQLException {
    BaseIT baseIT = new BaseIT();
    Config config = new Config(false) {};
    Path h2Path = null;
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
      try {
        h2Path = Files.createTempDirectory("gravitino_jdbc_test_h2_");
      } catch (IOException e) {
        throw new RuntimeException("Create H2 test directory failed", e);
      }

      config.set(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
          String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "123456");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    }

    JDBCBackend jdbcBackend = new JDBCBackend();
    try {
      // Close any leftover shared SQL session state before initializing the next backend.
      jdbcBackend.close();
    } catch (IOException e) {
      throw new RuntimeException("Close JDBC backend before initialization failed", e);
    }
    jdbcBackend.initialize(config);
    return new BackendResource(jdbcBackend, h2Path);
  }

  private static class BackendResource {
    private final JDBCBackend backend;
    private final Path h2Path;

    private BackendResource(JDBCBackend backend, Path h2Path) {
      this.backend = backend;
      this.h2Path = h2Path;
    }

    private JDBCBackend backend() {
      return backend;
    }

    private void close() throws Exception {
      backend.close();
      if (h2Path != null && Files.exists(h2Path)) {
        deleteDirectory(h2Path);
      }
    }
  }

  private static void deleteDirectory(Path dir) throws IOException {
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
