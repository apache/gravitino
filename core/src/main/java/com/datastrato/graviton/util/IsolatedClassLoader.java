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

public class IsolatedClassLoader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(IsolatedClassLoader.class);

  private final List<URL> execJars;

  private final List<String> sharedClasses;

  private final List<String> barrierClasses;

  private URLClassLoader classLoader;

  private final ClassLoader baseClassLoader;

  public IsolatedClassLoader(
      List<URL> execJars, List<String> sharedClasses, List<String> barrierClasses) {
    this.execJars = execJars;
    this.sharedClasses = sharedClasses;
    this.barrierClasses = barrierClasses;
    this.baseClassLoader = Thread.currentThread().getContextClassLoader();
  }

  public <T> T withClassLoader(ThrowableFunction<ClassLoader, T> fn) throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader());
    try {
      return fn.apply(classLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

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

    this.classLoader =
        new URLClassLoader(
            execJars.toArray(new URL[0]), Thread.currentThread().getContextClassLoader()) {
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

  private boolean isSharedClass(String name) {
    return name.startsWith("org.slf4j")
        || name.startsWith("org.apache.log4j")
        || name.startsWith("org.apache.logging.log4j")
        || name.startsWith("java.")
        || (name.startsWith("com.datastrato.graviton.")
            && !name.startsWith("com.datastrato.graviton.catalog.hive."))
        || sharedClasses.stream().anyMatch(name::startsWith);
  }

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
