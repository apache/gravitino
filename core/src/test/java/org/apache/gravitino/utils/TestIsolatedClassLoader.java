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

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.gravitino.connector.SupportsLightTableLoad;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestIsolatedClassLoader {

  private IsolatedClassLoader classLoader;
  private Method isCatalogClassMethod;

  @BeforeEach
  public void setUp() throws Exception {
    classLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    isCatalogClassMethod =
        IsolatedClassLoader.class.getDeclaredMethod("isCatalogClass", String.class);
    isCatalogClassMethod.setAccessible(true);
  }

  private boolean isCatalogClass(String name) throws Exception {
    return (boolean) isCatalogClassMethod.invoke(classLoader, name);
  }

  /**
   * A connector implements {@link SupportsLightTableLoad} inside its own ClassLoader while the
   * server tests the result with {@code instanceof}, so both have to end up with the same {@link
   * Class}. This packages a second copy of the interface into the isolated ClassLoader's own jar,
   * the way a connector's {@code libs} directory does, and checks the server's copy still wins.
   *
   * <p>Identical bytes are not enough for {@code instanceof}: a class defined by another
   * ClassLoader is a different {@code Class} even byte-for-byte, which is why this asserts identity
   * rather than equality.
   */
  @Test
  public void testConnectorMixinIsSharedWithTheServerClassLoader(@TempDir Path pkgDir)
      throws Exception {
    String name = SupportsLightTableLoad.class.getName();
    writeClassIntoJar(pkgDir.resolve("duplicate.jar"), name);

    IsolatedClassLoader isolated = IsolatedClassLoader.buildClassLoader(List.of(pkgDir.toString()));
    try {
      Class<?> loaded = isolated.withClassLoader(cl -> cl.loadClass(name));

      // Checked first so the assertion below cannot pass simply because the duplicate was never
      // there: the isolated ClassLoader can see its own copy, and delegates anyway.
      Assertions.assertNotNull(
          isolated.getInternalClassLoader().findResource(name.replace('.', '/') + ".class"),
          "the duplicate should be visible to the isolated ClassLoader");
      Assertions.assertSame(
          SupportsLightTableLoad.class,
          loaded,
          "A connector-local copy of the mixin must not shadow the server's, or the instanceof "
              + "check that routes a light load would silently be false");
    } finally {
      isolated.close();
    }
  }

  /**
   * The sharing above only holds while the server's ClassLoader can actually serve the class, which
   * means the interface has to ship in a module on the server's main classpath. Connector and
   * auxiliary-service modules are packaged into their own directories instead, so an interface
   * declared in one of those would be served from each package directory separately -- and tests,
   * which run everything in a single ClassLoader, would not notice.
   */
  @Test
  public void testConnectorMixinShipsWithTheServerModule() {
    String codeSource =
        SupportsLightTableLoad.class.getProtectionDomain().getCodeSource().getLocation().getPath();

    Assertions.assertTrue(
        codeSource.contains("/core/") || codeSource.contains("gravitino-core"),
        "SupportsLightTableLoad must stay in gravitino-core, which the server loads; it was "
            + "found in "
            + codeSource);
  }

  private static void writeClassIntoJar(Path jar, String className) throws Exception {
    String resource = className.replace('.', '/') + ".class";
    try (InputStream in =
            TestIsolatedClassLoader.class.getClassLoader().getResourceAsStream(resource);
        OutputStream out = Files.newOutputStream(jar);
        JarOutputStream jarOut = new JarOutputStream(out)) {
      Assertions.assertNotNull(in, "cannot read " + resource);
      jarOut.putNextEntry(new JarEntry(resource));
      in.transferTo(jarOut);
      jarOut.closeEntry();
    }
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
}
