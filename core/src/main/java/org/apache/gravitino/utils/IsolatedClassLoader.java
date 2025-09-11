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

import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IsolatedClassLoader provides a mechanism for creating an isolated class loader that allows
 * controlled loading of classes from specified jars and shared classes from the base class loader.
 */
public class IsolatedClassLoader implements Closeable {

  public static final Class<?> CUSTOM_CLASS_LOADER_CLASS =
      IsolatedClassLoader.CustomURLClassLoader.class;

  private static final Logger LOG = LoggerFactory.getLogger(IsolatedClassLoader.class);

  private final List<URL> execJars;

  private final List<String> sharedClasses;

  private final List<String> barrierClasses;

  private URLClassLoader classLoader;

  private final ClassLoader baseClassLoader;

  // Reference count to track the number of users of this class loader.
  private AtomicInteger refCount = new AtomicInteger(0);

  // Last access time in milliseconds. This value is used to determine when to clean up the class
  // loader when refCount drops to zero.
  private long lastAccessTime = Long.MAX_VALUE;

  public static final BiMap<ClassLoaderCacheKey, IsolatedClassLoader> CLASSLOADER_CACHE =
      Maps.synchronizedBiMap(HashBiMap.create());

  public static final ScheduledThreadPoolExecutor CLASS_LOADER_CLEANER =
      new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory());

  static {
    CLASS_LOADER_CLEANER.scheduleAtFixedRate(
        () -> {
          synchronized (CLASSLOADER_CACHE) {
            if (CLASSLOADER_CACHE.isEmpty()) {
              return;
            }

            Iterator<Entry<ClassLoaderCacheKey, IsolatedClassLoader>> iterator =
                CLASSLOADER_CACHE.entrySet().iterator();
            while (iterator.hasNext()) {
              Entry<ClassLoaderCacheKey, IsolatedClassLoader> entry = iterator.next();
              IsolatedClassLoader classLoader = entry.getValue();
              // If the class loader is not used for more than 60 seconds, we clean it up.
              // TODO make 60000 configurable.
              if (classLoader.getRefCount().get() <= 0
                  && (System.currentTimeMillis() - classLoader.lastAccessTime) > 60000) {
                iterator.remove();
                classLoader.close();
                classLoader.classLoader = null;
                LOG.info("Cleaned up classloader for key: {}", entry.getKey());
              }
            }
          }
        },
        10,
        10,
        TimeUnit.SECONDS);
  }

  public static class ClassLoaderCacheKey {
    private final String provider;
    private final String packagePath;
    private final Map<String, String> options;

    // Configurable that can decide which class loader to use. For example, we may
    // want to use different class loader for different authentication methods, then
    // we can add "auth.method" to the useful options. Currently, only `provider` and `package`
    // are used, please refer to `ClassLoaderCacheKey`.
    private static final Set<String> CLASS_LOADER_AFFECT_OPTIONS = Set.of();

    private Map<String, String> extractUsefulOptions(Map<String, String> options) {
      return options.entrySet().stream()
          .filter(e -> CLASS_LOADER_AFFECT_OPTIONS.contains(e.getKey()))
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private ClassLoaderCacheKey(String provider, String packagePath, Map<String, String> options) {
      this.provider = provider;
      this.packagePath = packagePath;
      this.options = extractUsefulOptions(options);
    }

    public static ClassLoaderCacheKey of(
        String provider, String packagePath, Map<String, String> options) {
      return new ClassLoaderCacheKey(provider, packagePath, options);
    }

    public static ClassLoaderCacheKey of(String provider, String packagePath) {
      return new ClassLoaderCacheKey(provider, packagePath, Collections.emptyMap());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || !(o instanceof ClassLoaderCacheKey)) {
        return false;
      }

      ClassLoaderCacheKey that = (ClassLoaderCacheKey) o;
      return Objects.equal(provider, that.provider)
          && Objects.equal(packagePath, that.packagePath)
          && Objects.equal(options, that.options);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(provider, packagePath, options);
    }
  }

  /**
   * Constructs an IsolatedClassLoader with the provided parameters.
   *
   * @param execJars List of URLs representing the executable jars to load classes from.
   * @param sharedClasses List of fully qualified class names to be shared with the base class
   *     loader.
   * @param barrierClasses List of fully qualified class names to be loaded in an isolated manner.
   */
  public IsolatedClassLoader(
      List<URL> execJars, List<String> sharedClasses, List<String> barrierClasses) {
    this.execJars = execJars;
    this.sharedClasses = sharedClasses;
    this.barrierClasses = barrierClasses;
    this.baseClassLoader = Thread.currentThread().getContextClassLoader();
  }

  @Nonnull
  public AtomicInteger getRefCount() {
    return refCount;
  }

  private static ThreadFactory newDaemonThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("IsolateClassloader-cleaner" + "-%d")
        .build();
  }

  /**
   * Executes the provided function within the isolated class loading context.
   *
   * @param fn The function to be executed within the isolated class loading context.
   * @param <T> The return type of the function.
   * @return The result of the executed function.
   * @throws Exception if an error occurs during the execution.
   */
  public <T> T withClassLoader(ThrowableFunction<ClassLoader, T> fn) throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader());
    try {
      return fn.apply(classLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  /**
   * Executes the provided function within the isolated class loading context and wraps any
   * exception, for more, please refer to {@link #withClassLoader(ThrowableFunction)}.
   *
   * @param fn The function to be executed within the isolated class loading context.
   * @param exceptionClass The exception class to be wrapped.
   * @param <E> The exception type.
   * @param <T> The type of value return by this method
   * @return The return value of the fn that executed within the classloader.
   */
  public <T, E extends RuntimeException> T withClassLoader(
      ThrowableFunction<ClassLoader, T> fn, Class<E> exceptionClass) {
    try {
      return withClassLoader(fn);
    } catch (Exception e) {
      if (exceptionClass.isInstance(e)) {
        throw (E) e;
      }
      throw new RuntimeException(e);
    }
  }

  public static IsolatedClassLoader buildClassLoader(List<String> libAndResourcesPaths) {
    // Listing all the classPath under the package path and build the isolated class loader.
    List<URL> classPathContents = Lists.newArrayList();
    for (String path : libAndResourcesPaths) {
      File folder = new File(path);
      if (!folder.exists() || !folder.isDirectory() || !folder.canRead() || !folder.canExecute()) {
        throw new IllegalArgumentException(
            String.format("Invalid package path: %s in %s", path, libAndResourcesPaths));
      }

      // Add all the jar under the folder to classpath.
      Arrays.stream(folder.listFiles())
          .filter(f -> f.getName().endsWith(".jar"))
          .forEach(
              f -> {
                try {
                  classPathContents.add(f.toURI().toURL());
                } catch (MalformedURLException e) {
                  LOG.warn("Failed to read jar file: {}", f.getAbsolutePath(), e);
                }
              });

      // Add itself to the classpath.
      try {
        classPathContents.add(folder.toURI().toURL());
      } catch (MalformedURLException e) {
        LOG.warn("Failed to read directory: {}", folder.getAbsolutePath(), e);
      }
    }

    return new IsolatedClassLoader(
        classPathContents, Collections.emptyList(), Collections.emptyList());
  }

  /** Closes the class loader. */
  @Override
  public void close() {
    try {
      if (classLoader != null) {
        // We will not close the class loader immediately as it may be used by later calls. If
        // the class loader is not used for more than 60 seconds, we will clean it up in the
        // background.
        if (refCount.decrementAndGet() <= 0) {
          lastAccessTime = System.currentTimeMillis();
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to close classloader", e);
    }
  }

  class CustomURLClassLoader extends URLClassLoader {
    private final ClassLoader baseClassLoader;

    public CustomURLClassLoader(URL[] urls, ClassLoader parent, ClassLoader baseClassLoader) {
      super(urls, parent);
      this.baseClassLoader = baseClassLoader;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      Class<?> clazz = findLoadedClass(name);

      try {
        return clazz == null ? doLoadClass(name, resolve) : clazz;
      } catch (Exception e) {
        throw new ClassNotFoundException("Class not found " + name, e);
      }
    }

    private Class<?> doLoadClass(String name, boolean resolve) throws Exception {
      if (isBarrierClass(name)) {
        // For barrier classes, copy the class bytecode and reconstruct the class.
        if (LOG.isDebugEnabled()) {
          LOG.debug("barrier class: {}", name);
        }
        byte[] bytes = loadClassBytes(name);
        return defineClass(name, bytes, 0, bytes.length);

      } else if (!isSharedClass(name)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("isolated class: {} - {}", name, getResources(classToPath(name)));
        }
        return super.loadClass(name, resolve);

      } else {
        // For shared classes, delegate to base classloader.
        if (LOG.isDebugEnabled()) {
          LOG.debug("shared class: {}", name);
        }
        try {
          return baseClassLoader.loadClass(name);
        } catch (ClassNotFoundException e) {
          // Fall through.
          return super.loadClass(name, resolve);
        }
      }
    }
  }

  private synchronized URLClassLoader classLoader() {
    if (classLoader != null) {
      return classLoader;
    }

    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    this.classLoader =
        new CustomURLClassLoader(execJars.toArray(new URL[0]), parent, baseClassLoader);
    return classLoader;
  }

  /**
   * Checks if a given class name belongs to a shared class.
   *
   * @param name The fully qualified class name.
   * @return true if the class is shared, false otherwise.
   */
  private boolean isSharedClass(String name) {
    return name.startsWith("org.slf4j")
        || name.startsWith("org.apache.log4j")
        || name.startsWith("org.apache.logging.log4j")
        || name.startsWith("java.")
        || !isCatalogClass(name)
        || sharedClasses.stream().anyMatch(name::startsWith);
  }

  /**
   * Checks if a given class name belongs to a catalog class.
   *
   * @param name The fully qualified class name.
   * @return true if the class is a catalog class, false otherwise.
   */
  private boolean isCatalogClass(String name) {
    return name.startsWith("org.apache.gravitino.catalog")
        && (name.startsWith("org.apache.gravitino.catalog.hive.")
            || name.startsWith("org.apache.gravitino.catalog.lakehouse.")
            || name.startsWith("org.apache.gravitino.catalog.jdbc.")
            || name.startsWith("org.apache.gravitino.catalog.mysql.")
            || name.startsWith("org.apache.gravitino.catalog.postgresql.")
            || name.startsWith("org.apache.gravitino.catalog.doris.")
            || name.startsWith("org.apache.gravitino.catalog.hadoop.")
            || name.startsWith("org.apache.gravitino.catalog.fileset.")
            || name.startsWith("org.apache.gravitino.catalog.model.")
            || name.startsWith("org.apache.gravitino.catalog.kafka."));
  }

  /**
   * Checks if a given class name belongs to a barrier class.
   *
   * @param name The fully qualified class name.
   * @return true if the class is a barrier class, false otherwise.
   */
  private boolean isBarrierClass(String name) {
    // We need to add more later on when we have more catalog implementations.
    return barrierClasses.stream().anyMatch(name::startsWith);
  }

  private ClassLoader getRootClassLoader() throws Exception {
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
      // In Java 9, the boot classloader can see few JDK classes. The intended parent
      // classloader for delegation is now the platform classloader.
      // See http://java9.wtf/class-loading/
      return (ClassLoader) ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
    } else {
      return null;
    }
  }

  private byte[] loadClassBytes(String name) throws Exception {
    try (InputStream io = baseClassLoader.getResourceAsStream(classToPath(name))) {
      if (io != null) {
        return IOUtils.toByteArray(io);
      } else {
        throw new ClassNotFoundException("Class not found: " + name);
      }
    }
  }

  private String classToPath(String name) {
    return name.replaceAll("\\.", "/") + ".class";
  }
}
