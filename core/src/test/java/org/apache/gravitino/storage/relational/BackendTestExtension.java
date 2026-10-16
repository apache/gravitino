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
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.support.AnnotationSupport;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs each {@code @TestTemplate} against a relational backend and injects it into {@link
 * TestJDBCBackend} tests.
 *
 * <p>The backend is the one {@link BackendTestSelector} names, which each core database lane sets.
 * With none selected, tests run on H2, plus MySQL and PostgreSQL when {@code dockerTest=true}.
 *
 * <p>{@code SqlSessionFactoryHelper} is process-global, so only one backend can be live in a JVM.
 * The extension keeps it in a single slot and reuses it for the tests of one class, emptying it
 * before each test per the class's {@link DatabaseFixture}. It builds a new backend when the test
 * class or backend type changes and for every {@link ResetStrategy#FRESH_DATABASE} test, and closes
 * the class's backend when the class finishes.
 *
 * <p>The slot is read and replaced without locking, so tests must run one at a time: the core
 * database tasks run in one JVM with JUnit parallel execution off.
 */
public class BackendTestExtension
    implements TestTemplateInvocationContextProvider, AfterAllCallback {

  private static final Logger LOG = LoggerFactory.getLogger(BackendTestExtension.class);
  private static final String DOCKER_TEST_FLAG = "dockerTest";

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(BackendTestExtension.class);
  private static final String CURRENT_BACKEND = "CURRENT_BACKEND";

  private final BackendFactory backendFactory;

  /** Creates the extension with the production database-backend factory. */
  public BackendTestExtension() {
    this(new DefaultBackendFactory());
  }

  BackendTestExtension(BackendFactory backendFactory) {
    this.backendFactory = backendFactory;
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    ExtensionContext.Store store = context.getRoot().getStore(NAMESPACE);
    Backend current = store.get(CURRENT_BACKEND, Backend.class);
    if (current != null && current.testClass == context.getRequiredTestClass()) {
      store.remove(CURRENT_BACKEND, Backend.class).close();
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
          testMethodName, Collections.singletonList(selectedBackend.get()));
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

    return createInvocationContexts(testMethodName, backendsToTest);
  }

  Stream<TestTemplateInvocationContext> createInvocationContexts(
      String testMethodName, List<String> backends) {
    return backends.stream()
        .map(backendType -> new BackendInvocationContext(testMethodName, backendType));
  }

  private class BackendInvocationContext implements TestTemplateInvocationContext {
    private final String testMethodName;
    private final String backendType;

    private BackendInvocationContext(String testMethodName, String backendType) {
      this.testMethodName = testMethodName;
      this.backendType = backendType;
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
      return Collections.singletonList(new BackendSetupCallback(backendType));
    }
  }

  @FunctionalInterface
  interface BackendFactory {
    Backend create(String backendType, Class<?> testClass) throws Exception;
  }

  private class BackendSetupCallback implements BeforeEachCallback, AfterEachCallback {
    private final String backendType;

    private BackendSetupCallback(String backendType) {
      this.backendType = backendType;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      ExtensionContext.Store store = context.getRoot().getStore(NAMESPACE);
      Class<?> testClass = context.getRequiredTestClass();
      Backend current = store.get(CURRENT_BACKEND, Backend.class);
      if (current != null
          && (freshDatabase(context)
              || current.testClass != testClass
              || !current.backendType.equals(backendType))) {
        // Only one backend can own the global SQL session, so the old one goes first.
        store.remove(CURRENT_BACKEND, Backend.class).close();
        current = null;
      }
      if (current == null) {
        current = backendFactory.create(backendType, testClass);
        store.put(CURRENT_BACKEND, current);
      }
      // Other core tests overwrite these globals, so point them back before every test.
      current.bindGravitinoEnv();
      // Runs before the test's own @BeforeEach, so its setup always starts from empty tables.
      current.reset(resetStrategy(context));

      Object testInstance = context.getRequiredTestInstance();
      if (testInstance instanceof TestJDBCBackend) {
        ((TestJDBCBackend) testInstance).setBackend(current.backend);
        ((TestJDBCBackend) testInstance).setBackendType(backendType);
      }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      if (freshDatabase(context)) {
        Backend fresh =
            context.getRoot().getStore(NAMESPACE).remove(CURRENT_BACKEND, Backend.class);
        if (fresh != null) {
          fresh.close();
        }
      }
    }

    private boolean freshDatabase(ExtensionContext context) {
      return resetStrategy(context) == ResetStrategy.FRESH_DATABASE;
    }

    private ResetStrategy resetStrategy(ExtensionContext context) {
      DatabaseFixture fixture = fixture(context);
      return fixture == null ? ResetStrategy.DELETE_ROWS : fixture.resetStrategy();
    }

    // A method-level fixture replaces the class-level one.
    @Nullable
    private DatabaseFixture fixture(ExtensionContext context) {
      return AnnotationSupport.findAnnotation(
              context.getRequiredTestMethod(), DatabaseFixture.class)
          .or(
              () ->
                  AnnotationSupport.findAnnotation(
                      context.getRequiredTestClass(), DatabaseFixture.class))
          .orElse(null);
    }
  }

  /**
   * The live backend. JUnit closes whatever is left in the root store when the run ends, in the
   * reverse order of insertion, so it closes before the containers that {@code
   * CloseContainerExtension} registered earlier.
   */
  static class Backend implements ExtensionContext.Store.CloseableResource {
    private final String backendType;
    private final RelationalBackend backend;
    @Nullable private final Config config;
    @Nullable private final File h2Directory;
    private final Class<?> testClass;

    Backend(
        String backendType,
        Class<?> testClass,
        RelationalBackend backend,
        @Nullable Config config,
        @Nullable File h2Directory) {
      this.backendType = backendType;
      this.testClass = testClass;
      this.backend = backend;
      this.config = config;
      this.h2Directory = h2Directory;
    }

    private void bindGravitinoEnv() throws IllegalAccessException {
      if (config == null) {
        return;
      }
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "idGenerator", RandomIdGenerator.INSTANCE, true);
      EntityIdService.initialize(new RelationalEntityStoreIdResolver());
    }

    void reset(ResetStrategy resetStrategy) throws SQLException {
      // A fresh database has nothing to reset; a backend without config is a unit-test fake.
      if (resetStrategy != ResetStrategy.FRESH_DATABASE && config != null) {
        DatabaseResetter.reset(backendType, resetStrategy);
      }
    }

    @Override
    public void close() throws IOException {
      LOG.info("Tearing down backend: {}", backendType);
      try {
        backend.close();
      } finally {
        if (h2Directory != null) {
          FileUtils.deleteDirectory(h2Directory);
        }
      }
    }
  }

  private static class DefaultBackendFactory implements BackendFactory {
    @Override
    public Backend create(String backendType, Class<?> testClass) throws Exception {
      BaseIT baseIT = new BaseIT();
      LOG.info("Initializing backend resource: {}", backendType);
      Config config = Mockito.mock(Config.class);
      Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
      Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
          .thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS))
          .thenReturn(DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_IDLE_CONNECTIONS)).thenReturn(10);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
          .thenReturn(DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_WAIT_MILLISECONDS);

      Mockito.when(config.get(CACHE_ENABLED)).thenReturn(true);
      File h2Directory = null;
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
        h2Directory = Files.createTempDirectory("gravitino_jdbc_test_h2_").toFile();
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
            .thenReturn(
                String.format(
                    "jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Directory.getPath()));
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123456");
        Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
      }

      RelationalBackend backend = new JDBCBackend();
      Backend created = new Backend(backendType, testClass, backend, config, h2Directory);
      try {
        created.bindGravitinoEnv();
        // Close the backend before initializing to make sure the singleton sqlSession is cleared.
        backend.close();
        backend.initialize(config);
        return created;
      } catch (Exception e) {
        try {
          created.close();
        } catch (IOException closeFailure) {
          e.addSuppressed(closeFailure);
        }
        throw e;
      }
    }
  }
}
