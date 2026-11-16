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

package org.apache.gravitino.utils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests class isolation and shared-class visibility across context-classloader changes. */
public class TestIsolatedClassLoader {

  private static final String PROVIDER = "org.apache.gravitino.oss.fixture.LazyProvider";
  private static final String EXTERNAL_CLASS = "org.example.classloader.ExternalClass";

  @TempDir private static Path tempDir;

  private static URL fixtureJar;

  private IsolatedClassLoader classLoader;
  private Method isCatalogClassMethod;

  /** Builds classes outside the application classpath, including a lazily resolved dependency. */
  @BeforeAll
  public static void createFixtureJar() throws IOException {
    Path sources = Files.createDirectories(tempDir.resolve("sources"));
    Path classes = Files.createDirectories(tempDir.resolve("classes"));
    Path provider = sources.resolve("LazyProvider.java");
    Path dependency = sources.resolve("LazyDependency.java");
    Path external = sources.resolve("ExternalClass.java");
    Files.write(
        provider,
        ("package org.apache.gravitino.oss.fixture;"
                + "public class LazyProvider {"
                + "  public String value() { return LazyDependency.value(); }"
                + "}")
            .getBytes(StandardCharsets.UTF_8));
    Files.write(
        dependency,
        ("package org.apache.gravitino.oss.fixture;"
                + "public class LazyDependency {"
                + "  public static String value() { return \"loaded\"; }"
                + "}")
            .getBytes(StandardCharsets.UTF_8));
    Files.write(
        external,
        ("package org.example.classloader;"
                + "public class ExternalClass {"
                + "  public static String value() { return \"external\"; }"
                + "}")
            .getBytes(StandardCharsets.UTF_8));
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    Assertions.assertNotNull(compiler, "The classloader tests require a JDK");
    Assertions.assertEquals(
        0,
        compiler.run(
            null,
            null,
            null,
            "-proc:none",
            "-d",
            classes.toString(),
            provider.toString(),
            dependency.toString(),
            external.toString()));
    Path jarPath = tempDir.resolve("classloader-fixture.jar");
    try (Stream<Path> files = Files.walk(classes);
        JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
      for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
        jar.putNextEntry(new JarEntry(classes.relativize(file).toString().replace('\\', '/')));
        Files.copy(file, jar);
        jar.closeEntry();
      }
    }
    fixtureJar = jarPath.toUri().toURL();
  }

  @BeforeEach
  public void setUp() throws Exception {
    classLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    isCatalogClassMethod =
        IsolatedClassLoader.class.getDeclaredMethod("isCatalogClass", String.class);
    isCatalogClassMethod.setAccessible(true);
  }

  @Test
  public void testClassNotFoundExceptionDoesNotWrapProbeMiss() {
    // Probe-driven callers such as Janino expect a plain ClassNotFoundException for misses
    // so they can continue trying the next candidate name.
    ClassNotFoundException exception =
        Assertions.assertThrows(
            ClassNotFoundException.class,
            () -> classLoader.withClassLoader(cl -> cl.loadClass("UnknownProbeClass")));

    Assertions.assertNull(exception.getCause());
  }

  @Test
  public void testHivePackageRecognizedAsCatalogClass() throws Exception {
    // org.apache.gravitino.hive.* was moved from catalog.hive.* by HiveClient refactoring.
    // These must be treated as catalog classes so they are loaded by the IsolatedClassLoader,
    // not the server classloader. Otherwise their compiler-generated $1 synthetic classes
    // (from switch-on-enum) fail to load and the JVM permanently caches the failure.
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.hive.client.HiveExceptionConverter"));
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.hive.client.HiveExceptionConverter$1"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.hive.client.HiveClientPool"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.hive.HiveClientFactory"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.hive.SomeOtherClass"));
  }

  @Test
  public void testCatalogHivePackageRecognizedAsCatalogClass() throws Exception {
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.hive.HiveCatalogCapability"));
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.hive.HiveCatalogCapability$1"));
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.hive.HiveCatalogOperations"));
  }

  @Test
  public void testOtherCatalogPackagesRecognizedAsCatalogClass() throws Exception {
    Assertions.assertTrue(
        isCatalogClass("org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.jdbc.JdbcCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.kafka.KafkaCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.fileset.FilesetCatalog"));
    Assertions.assertTrue(isCatalogClass("org.apache.gravitino.catalog.model.ModelCatalog"));
  }

  @Test
  public void testNonCatalogPackagesNotRecognizedAsCatalogClass() throws Exception {
    // Server-side / shared classes must NOT be treated as catalog classes.
    Assertions.assertFalse(isCatalogClass("org.apache.gravitino.connector.BaseCatalog"));
    Assertions.assertFalse(isCatalogClass("org.apache.gravitino.NameIdentifier"));
    Assertions.assertFalse(isCatalogClass("org.apache.gravitino.catalog.SomeSharedClass"));
    Assertions.assertFalse(isCatalogClass("java.lang.String"));
    Assertions.assertFalse(isCatalogClass("org.slf4j.Logger"));
  }

  /** A nested loader must keep its cold dependencies loadable after the context owner closes. */
  @Test
  public void testNestedLoaderSurvivesContextLoaderClose() throws Exception {
    IsolatedClassLoader nested;
    try (IsolatedClassLoader first = newFixtureLoader()) {
      loadProvider(first);
      nested = first.withClassLoader(ignored -> newFixtureLoader());
    }
    try (IsolatedClassLoader second = nested) {
      Assertions.assertEquals("loaded", loadValue(second));
    }
  }

  /** Independent isolated loaders must not reuse provider classes from an open context owner. */
  @Test
  public void testNestedLoaderDoesNotReuseContextProvider() throws Exception {
    try (IsolatedClassLoader first = newFixtureLoader()) {
      Class<?> firstProvider = loadProvider(first);
      try (IsolatedClassLoader second = first.withClassLoader(ignored -> newFixtureLoader())) {
        Class<?> secondProvider = first.withClassLoader(ignored -> loadProvider(second));
        Assertions.assertNotSame(firstProvider, secondProvider);
        Assertions.assertEquals("loaded", loadValue(second));
      }
    }
  }

  /** A later context switch must not change the parent of a loader created on a clean thread. */
  @Test
  public void testFirstUseIgnoresClosedContextLoader() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    ClassLoader closedContext;
    try (IsolatedClassLoader first = newFixtureLoader()) {
      closedContext = first.withClassLoader(cl -> cl.loadClass(PROVIDER).getClassLoader());
    }
    try (IsolatedClassLoader second = newFixtureLoader()) {
      Thread.currentThread().setContextClassLoader(closedContext);
      Assertions.assertEquals("loaded", loadValue(second));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  /** A thread still carrying a closed catalog loader must be able to initialize another catalog. */
  @Test
  public void testConstructionIgnoresClosedContextLoader() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    ClassLoader closedContext;
    try (IsolatedClassLoader first = newFixtureLoader()) {
      closedContext = first.withClassLoader(cl -> cl.loadClass(PROVIDER).getClassLoader());
    }
    try {
      Thread.currentThread().setContextClassLoader(closedContext);
      try (IsolatedClassLoader second = newFixtureLoader()) {
        Assertions.assertEquals("loaded", loadValue(second));
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  /** Explicit noncatalog context loaders continue to supply their shared application classes. */
  @Test
  public void testCustomContextLoaderRemainsVisible() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader custom = new URLClassLoader(new URL[] {fixtureJar}, original)) {
      Thread.currentThread().setContextClassLoader(custom);
      try (IsolatedClassLoader isolated =
          new IsolatedClassLoader(
              Collections.emptyList(), Collections.emptyList(), Collections.emptyList())) {
        Thread.currentThread().setContextClassLoader(original);
        Object value =
            isolated.withClassLoader(
                cl -> cl.loadClass(EXTERNAL_CLASS).getMethod("value").invoke(null));
        Assertions.assertEquals("external", value);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  /** A context cleared during resource cleanup falls back to the Gravitino application loader. */
  @Test
  public void testNullContextLoaderCanLoadSharedClasses() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(null);
      try (IsolatedClassLoader isolated = newFixtureLoader()) {
        Assertions.assertSame(
            ThrowableFunction.class,
            isolated.withClassLoader(cl -> cl.loadClass(ThrowableFunction.class.getName())));
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  /**
   * Releasing a different pool key must not make the surviving entry's cold classes unavailable.
   */
  @Test
  public void testNestedPoolEntrySurvivesDifferentKeyRelease() throws Exception {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      PooledClassLoaderEntry first =
          pool.acquire(
              new ClassLoaderKey("fileset", Map.of("fs.defaultFS", "file:///first")),
              this::newFixtureLoader);
      loadProvider(first.classLoader());
      PooledClassLoaderEntry second =
          first
              .classLoader()
              .withClassLoader(
                  ignored ->
                      pool.acquire(
                          new ClassLoaderKey("fileset", Map.of("fs.defaultFS", "file:///second")),
                          this::newFixtureLoader));
      pool.release(first);
      try {
        Assertions.assertEquals("loaded", loadValue(second.classLoader()));
      } finally {
        pool.release(second);
      }
    }
  }

  private boolean isCatalogClass(String name) throws Exception {
    return (boolean) isCatalogClassMethod.invoke(classLoader, name);
  }

  private IsolatedClassLoader newFixtureLoader() {
    return new IsolatedClassLoader(
        Collections.singletonList(fixtureJar), Collections.emptyList(), Collections.emptyList());
  }

  private Class<?> loadProvider(IsolatedClassLoader loader) throws Exception {
    return loader.withClassLoader(cl -> cl.loadClass(PROVIDER));
  }

  private Object loadValue(IsolatedClassLoader loader) throws Exception {
    return loader.withClassLoader(
        cl -> {
          Class<?> provider = cl.loadClass(PROVIDER);
          return provider
              .getMethod("value")
              .invoke(provider.getDeclaredConstructor().newInstance());
        });
  }
}
