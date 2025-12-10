/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.hive.client;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Isolated client loader for Hive Metastore clients. This class creates an isolated classloader
 * that loads Hive-specific classes from version-specific jar files while sharing common classes
 * with the base classloader.
 */
public final class HiveClientClassLoader extends URLClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(HiveClientClassLoader.class);

  public enum HiveVersion {
    HIVE2,
    HIVE3,
  }

  private final ClassLoader baseClassLoader;
  private final HiveVersion version;

  /**
   * Constructs an HiveClientClassLoader.
   *
   * @param version The Hive version
   * @param execJars List of jar file URLs to load
   * @param baseClassLoader The base classloader for shared classes
   */
  private HiveClientClassLoader(
      HiveVersion version, List<URL> execJars, ClassLoader baseClassLoader) {
    super(version.toString(), execJars.toArray(new URL[0]), null);
    Preconditions.checkArgument(version != null, "Hive version cannot be null");
    Preconditions.checkArgument(
        execJars != null && !execJars.isEmpty(), "Jar URLs cannot be null or empty");
    Preconditions.checkArgument(baseClassLoader != null, "Base classloader cannot be null");

    this.version = version;
    this.baseClassLoader = baseClassLoader;
  }

  public HiveVersion getHiveVersion() {
    return version;
  }

  /**
   * Creates a new {@link HiveClientClassLoader} instance for the given version.
   *
   * <p>This method does not perform any caching. Callers are responsible for managing and
   * optionally caching returned instances.
   *
   * @param hiveVersion The Hive version to create a loader for.
   * @param baseLoader The parent classloader to delegate shared classes to.
   * @return A new {@link HiveClientClassLoader} instance.
   */
  public static HiveClientClassLoader createLoader(HiveVersion hiveVersion, ClassLoader baseLoader)
      throws IOException {
    Path jarDir = getJarDirectory(hiveVersion);
    if (!Files.exists(jarDir) || !Files.isDirectory(jarDir)) {
      throw new IOException("Hive jar directory does not exist or is not a directory: " + jarDir);
    }

    List<URL> jars = loadJarUrls(jarDir);
    if (jars.isEmpty()) {
      throw new IOException("No jar files found in directory: " + jarDir);
    }

    return new HiveClientClassLoader(hiveVersion, jars, baseLoader);
  }

  /**
   * Gets the jar directory path for the specified Hive version.
   *
   * @param version The Hive version
   * @return The path to the jar directory
   */
  private static Path getJarDirectory(HiveVersion version) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(StringUtils.isNotEmpty(gravitinoHome), "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String libsDir = version == HiveVersion.HIVE2 ? "hive-metastore2-libs" : "hive-metastore3-libs";

    Path jarDir;
    if (testEnv) {
      // In test, hive metastore client jars are under the build directory.
      jarDir = Paths.get(gravitinoHome, "catalogs", libsDir, "build", "libs");
    } else {
      // In production, jars are placed under the hive catalog libs directory.
      jarDir = Paths.get(gravitinoHome, "catalogs", "hive", "libs", libsDir);
    }

    if (!Files.exists(jarDir) || !Files.isDirectory(jarDir)) {
      throw new GravitinoRuntimeException(
          "Cannot find Hive jar directory for version %s in directory %s", version, jarDir);
    }

    return jarDir.toAbsolutePath();
  }

  /**
   * Loads all jar file URLs from the specified directory.
   *
   * @param jarDir The directory containing jar files
   * @return A list of jar file URLs
   * @throws IOException If an I/O error occurs
   */
  private static List<URL> loadJarUrls(Path jarDir) throws IOException {
    try (var stream = Files.list(jarDir)) {
      return stream
          .filter(p -> p.toString().endsWith(".jar"))
          .map(
              p -> {
                try {
                  return p.toUri().toURL();
                } catch (Exception e) {
                  throw new GravitinoRuntimeException(e, "Failed to convert path to URL: %s", p);
                }
              })
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IOException("Failed to list jar files in directory: " + jarDir.toString(), e);
    }
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> loaded = findLoadedClass(name);
    if (loaded != null) {
      return loaded;
    }
    if (isBarrierClass(name)) {
      return loadBarrierClass(name);
    } else if (isSharedClass(name)) {
      return loadSharedClass(name, resolve);
    } else {
      LOG.debug("Classloader {} loading isolated class {}", getName(), name);
      return super.loadClass(name, resolve);
    }
  }

  private Class<?> loadBarrierClass(String name) throws ClassNotFoundException {
    LOG.debug("Classloader {} loading barrier class {}", getName(), name);
    String classFileName = name.replace(".", "/") + ".class";
    try (InputStream is = baseClassLoader.getResourceAsStream(classFileName)) {
      if (is == null) {
        throw new ClassNotFoundException("Cannot load barrier class: " + name);
      }
      byte[] bytes = is.readAllBytes();
      return defineClass(name, bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new ClassNotFoundException("Failed to load barrier class: " + name, e);
    }
  }

  private Class<?> loadSharedClass(String name, boolean resolve) throws ClassNotFoundException {
    LOG.debug("Classloader {} loading shared class {}", getName(), name);
    try {
      return baseClassLoader.loadClass(name);
    } catch (ClassNotFoundException e) {
      // Fallback to isolated classloader if not found in base
      return super.loadClass(name, resolve);
    }
  }

  /**
   * Checks if a class should be shared with the base classloader.
   *
   * @param name The fully qualified class name
   * @return true if the class should be shared, false otherwise
   */
  private boolean isSharedClass(String name) {
    // Shared logging classes
    if (name.startsWith("org.slf4j")
        || name.startsWith("org.apache.log4j")
        || name.startsWith("org.apache.logging.log4j")) {
      return true;
    }

    // Shared Hadoop classes (excluding Hive-specific ones)
    if (name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hive.")) {
      return true;
    }

    // Shared Google classes (excluding cloud-specific ones)
    if (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) {
      return true;
    }

    // Java standard library classes
    if (name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("com.sun.")
        || name.startsWith("org.ietf.jgss.")) {
      return true;
    }

    // Gravitino classes
    if (name.startsWith("org.apache.gravitino.")) {
      return true;
    }

    return false;
  }

  /**
   * Checks if a class is a barrier class that should be loaded in isolation.
   *
   * @param name The fully qualified class name
   * @return true if the class is a barrier class, false otherwise
   */
  private boolean isBarrierClass(String name) {
    return name.startsWith(HiveClientImpl.class.getName())
        || name.startsWith(HiveShim.class.getName())
        || name.startsWith(Util.class.getName())
        || name.startsWith("org.apache.gravitino.hive.converter.");
  }
}
