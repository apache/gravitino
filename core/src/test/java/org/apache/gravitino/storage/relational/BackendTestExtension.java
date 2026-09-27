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

import static org.apache.gravitino.Configs.CACHE_ENABLED;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.TestWatcher;
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

  private final BackendFactory backendFactory;

  /** Creates the extension with the production database-backend factory. */
  public BackendTestExtension() {
    this(new DefaultBackendFactory());
  }

  BackendTestExtension(BackendFactory backendFactory) {
    this.backendFactory = backendFactory;
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    context.getStore(NAMESPACE).put(STORE_KEY, new ConcurrentHashMap<String, BackendResource>());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void afterAll(ExtensionContext context) throws Exception {
    ConcurrentHashMap<String, BackendResource> map =
        (ConcurrentHashMap<String, BackendResource>) context.getStore(NAMESPACE).remove(STORE_KEY);

    if (map != null) {
      Exception failure = null;
      for (BackendResource resource : map.values()) {
        try {
          resource.close();
        } catch (Exception e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    String testMethodName = context.getRequiredTestMethod().getName();
    Optional<String> selectedBackend = BackendTestSelector.selectedBackend();
    if (selectedBackend.isPresent()) {
      LOG.info("Running tests with the selected {} backend.", selectedBackend.get());
      return createInvocationContexts(
          testMethodName, Collections.singletonList(selectedBackend.get()), true);
    }

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

    return createInvocationContexts(testMethodName, backendsToTest, false);
  }

  Stream<TestTemplateInvocationContext> createInvocationContexts(
      String testMethodName, List<String> backends, boolean reuseBackend) {
    return backends.stream()
        .map(
            backendType ->
                new BackendInvocationContext(
                    testMethodName, backendType, reuseBackend, backendFactory));
  }

  private static class BackendInvocationContext implements TestTemplateInvocationContext {
    private final String testMethodName;
    private final String backendType;
    private final boolean reuseBackend;
    private final BackendFactory backendFactory;

    private BackendInvocationContext(
        String testMethodName,
        String backendType,
        boolean reuseBackend,
        BackendFactory backendFactory) {
      this.testMethodName = testMethodName;
      this.backendType = backendType;
      this.reuseBackend = reuseBackend;
      this.backendFactory = backendFactory;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
      // No trailing "()" here: @TestTemplate methods can declare parameters (e.g. an injected
      // DatabaseTestContext), and a hardcoded empty parameter list would misrepresent the
      // method's actual signature in JUnit XML/HTML reports.
      return String.format("%s[%s Backend]", testMethodName, backendType.toUpperCase());
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
      return Collections.singletonList(
          new BackendSetupCallback(backendType, reuseBackend, backendFactory));
    }
  }

  @FunctionalInterface
  interface BackendFactory {
    BackendResource create(String backendType) throws Exception;
  }

  @FunctionalInterface
  interface BackendActivator {
    void activate() throws Exception;
  }

  private static class DefaultBackendFactory implements BackendFactory {
    @Override
    public BackendResource create(String backendType) throws Exception {
      BaseIT baseIT = new BaseIT();
      LOG.info("Initializing backend resource: {}", backendType);
      Config config = Mockito.mock(Config.class);
      Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
          .thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS))
          .thenReturn(DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
          .thenReturn(DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_WAIT_MILLISECONDS);

      Mockito.when(config.get(CACHE_ENABLED)).thenReturn(true);
      RelationalBackend backend = new JDBCBackend();
      if ("mysql".equals(backendType)) {
        String url = baseIT.startAndInitMySQLBackend();
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(url);
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
            .thenReturn("com.mysql.cj.jdbc.Driver");
      } else if ("postgresql".equals(backendType)) {
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
        backend = new H2BackendWrapper(jdbcPath);
      }

      BackendResource resource =
          new BackendResource(
              backendType,
              backend,
              () -> {
                FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
                FieldUtils.writeField(
                    GravitinoEnv.getInstance(), "idGenerator", RandomIdGenerator.INSTANCE, true);
                EntityIdService.initialize(new RelationalEntityStoreIdResolver());
              });
      try {
        resource.activate();
        // Close the backend before initializing to make sure the singleton sqlSession is cleared.
        backend.close();
        backend.initialize(config);
        return resource;
      } catch (Exception failure) {
        try {
          resource.close();
        } catch (Exception closeFailure) {
          failure.addSuppressed(closeFailure);
        }
        throw failure;
      }
    }
  }

  private static class BackendSetupCallback
      implements BeforeEachCallback,
          AfterEachCallback,
          LifecycleMethodExecutionExceptionHandler,
          ParameterResolver,
          TestWatcher {
    private final String backendType;
    private final boolean reuseBackend;
    private final BackendFactory backendFactory;
    private BackendResource backendResource;
    private DatabaseTestContext databaseTestContext;
    private boolean closeAfterEach;

    private BackendSetupCallback(
        String backendType, boolean reuseBackend, BackendFactory backendFactory) {
      this.backendType = backendType;
      this.reuseBackend = reuseBackend;
      this.backendFactory = backendFactory;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      DatabaseIsolation isolation = isolation(context);
      if (isolation == DatabaseIsolation.DEDICATED_SERVER) {
        throw new UnsupportedOperationException(
            "DEDICATED_SERVER isolation is not implemented for core database tests");
      }

      closeAfterEach = !reuseBackend || isolation == DatabaseIsolation.FRESH_NAMESPACE;
      try {
        if (reuseBackend && isolation == DatabaseIsolation.FRESH_NAMESPACE) {
          closeClassBackend(context);
        }
        backendResource = closeAfterEach ? newBackendResource() : getOrCreateClassBackend(context);
        backendResource.activate();
        databaseTestContext =
            new DatabaseTestContext(backendType, backendResource.backend, isolation);
      } catch (Exception e) {
        cleanupAfterSetupFailure(e);
        throw e;
      }
    }

    @Override
    public boolean supportsParameter(
        ParameterContext parameterContext, ExtensionContext extensionContext) {
      return parameterContext.getParameter().getType() == DatabaseTestContext.class;
    }

    @Override
    public DatabaseTestContext resolveParameter(
        ParameterContext parameterContext, ExtensionContext extensionContext) {
      if (databaseTestContext == null) {
        throw new ParameterResolutionException(
            "DatabaseTestContext is unavailable before the database fixture starts");
      }
      return databaseTestContext;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      try {
        if (closeAfterEach && backendResource != null) {
          backendResource.close();
          backendResource = null;
        }
      } finally {
        databaseTestContext = null;
      }
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
      poisonSharedFixture();
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
      poisonSharedFixture();
    }

    @Override
    public void handleBeforeEachMethodExecutionException(
        ExtensionContext context, Throwable throwable) throws Throwable {
      poisonSharedFixture();
      throw throwable;
    }

    @Override
    public void handleAfterEachMethodExecutionException(
        ExtensionContext context, Throwable throwable) throws Throwable {
      poisonSharedFixture();
      throw throwable;
    }

    private BackendResource getOrCreateClassBackend(ExtensionContext context) throws Exception {
      ConcurrentHashMap<String, BackendResource> backendMap = backendMap(context);
      synchronized (backendMap) {
        BackendResource resource = backendMap.get(backendType);
        if (resource != null && resource.poisoned) {
          backendMap.remove(backendType);
          resource.close();
          resource = null;
        }
        if (resource == null) {
          resource = newBackendResource();
          backendMap.put(backendType, resource);
        }
        return resource;
      }
    }

    private void closeClassBackend(ExtensionContext context) throws Exception {
      ConcurrentHashMap<String, BackendResource> backendMap = backendMap(context);
      synchronized (backendMap) {
        BackendResource resource = backendMap.remove(backendType);
        if (resource != null) {
          resource.close();
        }
      }
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, BackendResource> backendMap(ExtensionContext context) {
      ExtensionContext classContext = context;
      while (classContext.getTestMethod().isPresent()) {
        classContext =
            classContext
                .getParent()
                .orElseThrow(() -> new IllegalStateException("Test class context is unavailable"));
      }

      ConcurrentHashMap<String, BackendResource> backendMap =
          (ConcurrentHashMap<String, BackendResource>)
              classContext.getStore(NAMESPACE).get(STORE_KEY);
      if (backendMap == null) {
        throw new IllegalStateException("Backend fixture store is unavailable");
      }
      return backendMap;
    }

    private BackendResource newBackendResource() throws Exception {
      return backendFactory.create(backendType);
    }

    private void cleanupAfterSetupFailure(Exception failure) {
      databaseTestContext = null;
      if (backendResource == null) {
        return;
      }
      if (!closeAfterEach) {
        backendResource.poisoned = true;
        return;
      }
      try {
        backendResource.close();
      } catch (Exception closeFailure) {
        failure.addSuppressed(closeFailure);
      } finally {
        backendResource = null;
      }
    }

    private void poisonSharedFixture() {
      if (!closeAfterEach && backendResource != null) {
        backendResource.poisoned = true;
      }
    }

    private DatabaseIsolation isolation(ExtensionContext context) {
      DatabaseFixture methodFixture =
          context.getRequiredTestMethod().getAnnotation(DatabaseFixture.class);
      if (methodFixture != null) {
        return methodFixture.value();
      }
      DatabaseFixture classFixture =
          context.getRequiredTestClass().getAnnotation(DatabaseFixture.class);
      return classFixture == null ? DatabaseIsolation.RESETTABLE_NAMESPACE : classFixture.value();
    }
  }

  static class BackendResource {
    private final String backendType;
    private final RelationalBackend backend;
    private final BackendActivator activator;
    private boolean poisoned;

    BackendResource(String backendType, RelationalBackend backend, BackendActivator activator) {
      this.backendType = backendType;
      this.backend = backend;
      this.activator = activator;
    }

    private void activate() throws Exception {
      activator.activate();
    }

    private void close() throws Exception {
      LOG.info("Tearing down backend: {}", backendType);
      Exception failure = null;
      try {
        backend.close();
      } catch (Exception e) {
        failure = e;
      }
      try {
        if (backend instanceof H2BackendWrapper) {
          ((H2BackendWrapper) backend).cleanFile();
        }
      } catch (Exception e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  // A simple Wrapper solves the H2 file cleanup issue.
  public static class H2BackendWrapper extends JDBCBackend {
    private final String path;

    public H2BackendWrapper(String path) {
      this.path = path;
    }

    public void cleanFile() throws IOException {
      if (Files.exists(Paths.get(path))) {
        FileUtils.deleteDirectory(new File(path));
      }
    }
  }
}
