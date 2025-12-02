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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Isolated client loader for Hive Metastore clients. This class creates an isolated classloader
 * that loads Hive-specific classes from version-specific jar files while sharing common classes
 * with the base classloader.
 */
public final class IsolatedClientLoader {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedClientLoader.class);

  private static final String RETRYING_META_STORE_CLIENT_CLASS =
      "org.apache.hadoop.hive.metastore.RetryingMetaStoreClient";
  private static final String HIVE_CONF_CLASS = "org.apache.hadoop.hive.conf.HiveConf";
  private static final String CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";
  private static final String METHOD_GET_PROXY = "getProxy";
  private static final String METHOD_SET = "set";

  public enum HiveVersion {
    HIVE2,
    HIVE3,
  }

  private final HiveVersion version;
  private final List<URL> execJars;
  private final ClassLoader baseClassLoader;
  private final URLClassLoader classLoader;

  /** Cache IsolatedClientLoader instances per Hive version to avoid re-creating classloaders. */
  private static final Map<HiveVersion, IsolatedClientLoader> LOADER_CACHE =
      new EnumMap<>(HiveVersion.class);

  private static synchronized IsolatedClientLoader getClientLoaderForVersion(
      HiveVersion hiveVersion, Properties config) throws IOException {
    // Reuse loader per version to avoid repeatedly creating isolated classloaders.
    IsolatedClientLoader cached = LOADER_CACHE.get(hiveVersion);
    if (cached != null) {
      return cached;
    }

    IsolatedClientLoader loader =
        createLoader(hiveVersion, config, Thread.currentThread().getContextClassLoader());
    LOADER_CACHE.put(hiveVersion, loader);
    return loader;
  }

  public static HiveClient createHiveClient(Properties properties) {
    HiveClient client = null;
    try {
      // try using Hive3 first
      IsolatedClientLoader loader = getClientLoaderForVersion(HiveVersion.HIVE3, properties);
      client = loader.createClient(properties);
      client.getCatalogs();
      LOG.info("Connected to Hive Metastore using Hive version HIVE3");
      return client;

    } catch (GravitinoRuntimeException e) {
      if (client != null) {
        client.close();
      }

      try {
        // failback to Hive2 if we can list databases
        if (e.getMessage().contains("Invalid method name: 'get_catalogs'")
            || e.getMessage().contains("class not found") // cause by MiniHiveMetastoreService
        ) {
          IsolatedClientLoader loader = getClientLoaderForVersion(HiveVersion.HIVE2, properties);
          client = loader.createClient(properties);
          LOG.info("Connected to Hive Metastore using Hive version HIVE2");
          return client;
        }
        throw e;
      } catch (Exception ex) {
        LOG.error("Failed to connect to Hive Metastore using both Hive3 and Hive2", ex);
        throw e;
      }
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, "");
    }
  }

  private static IsolatedClientLoader createLoader(
      HiveVersion hiveVersion, Properties config, ClassLoader baseLoader) throws IOException {
    Path jarDir = getJarDirectory(hiveVersion);
    if (!Files.exists(jarDir) || !Files.isDirectory(jarDir)) {
      throw new IOException("Hive jar directory does not exist or is not a directory: " + jarDir);
    }

    List<URL> jars = loadJarUrls(jarDir);
    if (jars.isEmpty()) {
      throw new IOException("No jar files found in directory: " + jarDir);
    }

    return new IsolatedClientLoader(hiveVersion, jars, config, baseLoader);
  }

  /**
   * Gets the jar directory path for the specified Hive version.
   *
   * @param version The Hive version
   * @return The path to the jar directory
   */
  private static Path getJarDirectory(HiveVersion version) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (StringUtils.isEmpty(gravitinoHome)) {
      Path p = Paths.get(System.getProperty("user.dir"));
      while (p != null) {
        if (Files.exists(p.resolve("catalogs").resolve("hive-metastore-common"))) {
          gravitinoHome = p.toString();
          break;
        }
        p = p.getParent();
      }
    }

    if (StringUtils.isEmpty(gravitinoHome)) {
      throw new GravitinoRuntimeException(
          "GRAVITINO_HOME environment variable is not set and cannot determine project root directory");
    }

    String libsDir = version == HiveVersion.HIVE2 ? "hive-metastore2-libs" : "hive-metastore3-libs";

    // try to get path from GRAVITINO_HOME in deployment mode
    String jarPath = Paths.get(gravitinoHome, "catalogs", "hive", "libs", libsDir).toString();
    if (Files.exists(Paths.get(jarPath))) {
      return Paths.get(jarPath).toAbsolutePath();
    }
    LOG.info("Can not find Hive jar directory for version {} in directory : {}", version, jarPath);

    // Try to get project root directory from project root in development mode
    jarPath = Paths.get(gravitinoHome, "catalogs", libsDir, "build", "libs").toString();
    if (!Files.exists(Paths.get(jarPath))) {
      throw new GravitinoRuntimeException(
          "Cannot find Hive jar directory for version %s in directory <PROJECT_HOME>/catalogs/%s/build/libs "
              + "or $GRAVITINO_HOME/catalogs/hive/libs/%s",
          version, libsDir, libsDir);
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

  /**
   * Constructs an IsolatedClientLoader.
   *
   * @param version The Hive version
   * @param execJars List of jar file URLs to load
   * @param config Configuration map (reserved for future use)
   * @param baseClassLoader The base classloader for shared classes
   */
  public IsolatedClientLoader(
      HiveVersion version, List<URL> execJars, Properties config, ClassLoader baseClassLoader) {
    Preconditions.checkArgument(version != null, "Hive version cannot be null");
    Preconditions.checkArgument(
        execJars != null && !execJars.isEmpty(), "Jar URLs cannot be null or empty");
    Preconditions.checkArgument(baseClassLoader != null, "Base classloader cannot be null");

    this.version = version;
    this.execJars = Collections.unmodifiableList(execJars);
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
      throw HiveExceptionConverter.toGravitinoException(
          e, properties.getProperty("hive.metastore.uris"));
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

    if (version == HiveVersion.HIVE2) {
      return createHive2Client(clientClass, hiveConfClass, confClass, properties);
    } else if (version == HiveVersion.HIVE3) {
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
  /** Internal helper to close this loader's classloader. */
  private void closeInternal() throws IOException {
    try {
      classLoader.close();
    } catch (Exception e) {
      LOG.warn("Failed to close isolated Hive classloader", e);
      throw new IOException("Failed to close isolated Hive classloader", e);
    }
  }

  /** Shutdown for all cached IsolatedClientLoader instances. */
  public static synchronized void shutdown() {
    for (IsolatedClientLoader loader : LOADER_CACHE.values()) {
      try {
        loader.closeInternal();
      } catch (IOException e) {
        LOG.warn("Failed to close isolated Hive classloader for version {}", loader.version, e);
      }
    }
    LOADER_CACHE.clear();
  }
}
