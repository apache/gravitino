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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Isolated client loader for Hive Metastore clients. This class creates an isolated classloader
 * that loads Hive-specific classes from version-specific jar files while sharing common classes
 * with the base classloader.
 */
public final class IsolatedClientLoader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedClientLoader.class);

  private static final String RETRYING_META_STORE_CLIENT_CLASS =
      "org.apache.hadoop.hive.metastore.RetryingMetaStoreClient";
  private static final String HIVE_CONF_CLASS = "org.apache.hadoop.hive.conf.HiveConf";
  private static final String CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";
  private static final String METHOD_GET_PROXY = "getProxy";
  private static final String METHOD_SET = "set";

  /**
   * Creates isolated Hive client loaders by loading jars from the specified version directory.
   *
   * @param hiveMetastoreVersion The Hive version (HIVE2 or HIVE3)
   * @param config Configuration map (currently unused, reserved for future use)
   * @return An IsolatedClientLoader instance
   * @throws IOException If an I/O error occurs while loading jars
   */
  public static synchronized IsolatedClientLoader forVersion(
      String hiveMetastoreVersion, Map<String, String> config) throws IOException {
    Preconditions.checkArgument(
        hiveMetastoreVersion != null && !hiveMetastoreVersion.isEmpty(),
        "Hive metastore version cannot be null or empty");

    HiveClient.HiveVersion hiveVersion;
    try {
      hiveVersion = HiveClient.HiveVersion.valueOf(hiveMetastoreVersion);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported Hive version: " + hiveMetastoreVersion, e);
    }

    Path jarDir = getJarDirectory(hiveVersion);
    if (!Files.exists(jarDir) || !Files.isDirectory(jarDir)) {
      throw new IOException("Hive jar directory does not exist or is not a directory: " + jarDir);
    }

    List<URL> jars = loadJarUrls(jarDir);
    if (jars.isEmpty()) {
      throw new IOException("No jar files found in directory: " + jarDir);
    }

    return new IsolatedClientLoader(
        hiveVersion, jars, config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Gets the jar directory path for the specified Hive version.
   *
   * @param version The Hive version
   * @return The path to the jar directory
   */
  private static Path getJarDirectory(HiveClient.HiveVersion version) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String jarPath;
    if (testEnv && gravitinoHome != null) {
      // In test environment, use GRAVITINO_HOME
      String libsDir =
          version == HiveClient.HiveVersion.HIVE2 ? "hive-metastore2-libs" : "hive-metastore3-libs";
      jarPath = Paths.get(gravitinoHome, "catalogs", libsDir, "build", "libs").toString();
    } else {
      // Try to use relative path from current working directory
      String libsDir =
          version == HiveClient.HiveVersion.HIVE2 ? "hive-metastore2-libs" : "hive-metastore3-libs";
      jarPath = Paths.get("catalogs", libsDir, "build", "libs").toString();
    }

    return Paths.get(jarPath).toAbsolutePath();
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

  private final HiveClient.HiveVersion version;
  private final List<URL> execJars;

  @SuppressWarnings("UnusedVariable")
  private final Map<String, String> config;

  private final ClassLoader baseClassLoader;
  private final URLClassLoader classLoader;

  /**
   * Constructs an IsolatedClientLoader.
   *
   * @param version The Hive version
   * @param execJars List of jar file URLs to load
   * @param config Configuration map (reserved for future use)
   * @param baseClassLoader The base classloader for shared classes
   */
  public IsolatedClientLoader(
      HiveClient.HiveVersion version,
      List<URL> execJars,
      Map<String, String> config,
      ClassLoader baseClassLoader) {
    Preconditions.checkArgument(version != null, "Hive version cannot be null");
    Preconditions.checkArgument(
        execJars != null && !execJars.isEmpty(), "Jar URLs cannot be null or empty");
    Preconditions.checkArgument(baseClassLoader != null, "Base classloader cannot be null");

    this.version = version;
    this.execJars = Collections.unmodifiableList(execJars);
    this.config = config != null ? Collections.unmodifiableMap(config) : Collections.emptyMap();
    this.baseClassLoader = baseClassLoader;
    this.classLoader = createClassLoader();
  }

  /**
   * Creates an isolated URLClassLoader that loads classes with proper isolation and sharing rules.
   *
   * @return The created URLClassLoader
   */
  private URLClassLoader createClassLoader() {
    return new URLClassLoader(version.name(), execJars.toArray(new URL[0]), null) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> loaded = findLoadedClass(name);
        if (loaded != null) {
          return loaded;
        }
        return doLoadClass(name, resolve);
      }

      private Class<?> doLoadClass(String name, boolean resolve) throws ClassNotFoundException {
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
    };
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
    if (name.startsWith("org.apache.gravitino.") || name.startsWith(HiveClient.class.getName())) {
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
        || name.startsWith(Shim.class.getName())
        || name.startsWith(HiveShimV2.class.getName())
        || name.startsWith(HiveShimV3.class.getName())
        || name.startsWith(Util.class.getName())
        || name.startsWith("org.apache.gravitino.hive.converter.");
  }

  /**
   * Creates a Hive client instance using the isolated classloader.
   *
   * @param properties Hive configuration properties
   * @return A HiveClient instance
   * @throws Exception If client creation fails
   */
  public HiveClient createClient(Properties properties) throws Exception {
    Preconditions.checkArgument(properties != null, "Properties cannot be null");

    ClassLoader origLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      Object metastoreClient = createMetastoreClient(properties);
      return createHiveClientImpl(metastoreClient);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(origLoader);
    }
  }

  /**
   * Creates a Hive Metastore client instance based on the version.
   *
   * @param properties Hive configuration properties
   * @return The Metastore client object
   * @throws Exception If client creation fails
   */
  private Object createMetastoreClient(Properties properties) throws Exception {
    Class<?> clientClass = classLoader.loadClass(RETRYING_META_STORE_CLIENT_CLASS);
    Class<?> hiveConfClass = classLoader.loadClass(HIVE_CONF_CLASS);
    Class<?> confClass = classLoader.loadClass(CONFIGURATION_CLASS);

    if (version == HiveClient.HiveVersion.HIVE2) {
      return createHive2Client(clientClass, hiveConfClass, confClass, properties);
    } else if (version == HiveClient.HiveVersion.HIVE3) {
      return createHive3Client(clientClass, confClass, properties);
    } else {
      throw new IllegalArgumentException("Unsupported Hive version: " + version);
    }
  }

  /**
   * Creates a Hive 2.x Metastore client.
   *
   * @param clientClass The RetryingMetaStoreClient class
   * @param hiveConfClass The HiveConf class
   * @param confClass The Configuration class
   * @param properties Configuration properties
   * @return The Metastore client instance
   * @throws Exception If client creation fails
   */
  private Object createHive2Client(
      Class<?> clientClass, Class<?> hiveConfClass, Class<?> confClass, Properties properties)
      throws Exception {
    Object conf = confClass.getDeclaredConstructor().newInstance();
    Constructor<?> hiveConfCtor = hiveConfClass.getConstructor(confClass, Class.class);
    Object hiveConfInstance = hiveConfCtor.newInstance(conf, hiveConfClass);

    setConfigurationProperties(hiveConfInstance, confClass, properties);

    Method getProxyMethod = clientClass.getMethod(METHOD_GET_PROXY, hiveConfClass, boolean.class);
    return getProxyMethod.invoke(null, hiveConfInstance, false);
  }

  /**
   * Creates a Hive 3.x Metastore client.
   *
   * @param clientClass The RetryingMetaStoreClient class
   * @param confClass The Configuration class
   * @param properties Configuration properties
   * @return The Metastore client instance
   * @throws Exception If client creation fails
   */
  private Object createHive3Client(Class<?> clientClass, Class<?> confClass, Properties properties)
      throws Exception {
    Object conf = confClass.getDeclaredConstructor().newInstance();

    setConfigurationProperties(conf, confClass, properties);

    Method getProxyMethod = clientClass.getMethod(METHOD_GET_PROXY, confClass, boolean.class);
    return getProxyMethod.invoke(null, conf, true);
  }

  /**
   * Sets configuration properties on a configuration object.
   *
   * @param configObject The configuration object
   * @param confClass The Configuration class
   * @param properties The properties to set
   * @throws Exception If setting properties fails
   */
  private void setConfigurationProperties(
      Object configObject, Class<?> confClass, Properties properties) throws Exception {
    Method setMethod = confClass.getMethod(METHOD_SET, String.class, String.class);
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      setMethod.invoke(configObject, entry.getKey(), entry.getValue());
    }
  }

  /**
   * Creates a HiveClientImpl instance wrapping the Metastore client.
   *
   * @param metastoreClient The Metastore client instance
   * @return A HiveClient instance
   * @throws Exception If client creation fails
   */
  private HiveClient createHiveClientImpl(Object metastoreClient) throws Exception {
    Class<?> clientImplClass = classLoader.loadClass(HiveClientImpl.class.getName());
    Constructor<?> constructor = clientImplClass.getConstructors()[0];
    return (HiveClient) constructor.newInstance(version, metastoreClient);
  }

  /**
   * Closes the classloader and releases resources.
   *
   * @throws IOException If an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    try {
      classLoader.close();
    } catch (Exception e) {
      LOG.warn("Failed to close classloader", e);
      throw new IOException("Failed to close classloader", e);
    }
  }
}
