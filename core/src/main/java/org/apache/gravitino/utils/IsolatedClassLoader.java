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

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
        classLoader.close();
        classLoader = null;
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
