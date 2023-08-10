/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.util;

import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.rel.BaseSchema;
import java.io.Closeable;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
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

  /** Closes the class loader. */
  @Override
  public void close() {
    try {
      if (classLoader != null) {
        classLoader.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close classloader", e);
    }
  }

  private synchronized URLClassLoader classLoader() throws Exception {
    if (classLoader != null) {
      return classLoader;
    }

    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    this.classLoader =
        new URLClassLoader(execJars.toArray(new URL[0]), parent) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            Class<?> clazz = findLoadedClass(name);

            try {
              return clazz == null ? doLoadClass(name, resolve) : clazz;
            } catch (Exception e) {
              throw new ClassNotFoundException("Class no found " + name, e);
            }
          }

          private Class<?> doLoadClass(String name, boolean resolve) throws Exception {
            if (isBarrierClass(name)) {
              // For barrier classes, copy the class bytecode and reconstruct the class.
              LOG.debug("barrier class: {}", name);
              byte[] bytes = loadClassBytes(name);
              return defineClass(name, bytes, 0, bytes.length);

            } else if (!isSharedClass(name)) {
              LOG.debug("isolated class: {} - {}", name, getResources(classToPath(name)));
              return super.loadClass(name, resolve);

            } else {
              // For shared classes, delegate to base classloader.
              LOG.debug("shared class: {}", name);
              try {
                return baseClassLoader.loadClass(name);
              } catch (ClassNotFoundException e) {
                // Fall through.
                return super.loadClass(name, resolve);
              }
            }
          }
        };

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
        || (name.startsWith("com.datastrato.graviton.")
            && !name.startsWith("com.datastrato.graviton.catalog.hive."))
        || sharedClasses.stream().anyMatch(name::startsWith);
  }

  /**
   * Checks if a given class name belongs to a barrier class.
   *
   * @param name The fully qualified class name.
   * @return true if the class is a barrier class, false otherwise.
   */
  private boolean isBarrierClass(String name) {
    // We need to add more later on when we have more catalog implementations.
    return name.startsWith(BaseSchema.class.getName())
        || name.startsWith(AuditInfo.class.getName())
        || barrierClasses.stream().anyMatch(name::startsWith);
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
