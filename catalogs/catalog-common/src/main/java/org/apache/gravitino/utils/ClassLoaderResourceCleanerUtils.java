/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Provider;
import java.security.Security;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.ResourceBundle;
import java.util.Timer;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to clean up resources related to a specific class loader to prevent memory leaks.
 * Gravitino will create a new class loader for each catalog and release it when there exist any
 * changes to the catalog. So, it's important to clean up resources related to the class loader to
 * prevent memory leaks.
 */
public class ClassLoaderResourceCleanerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderResourceCleanerUtils.class);

  private ClassLoaderResourceCleanerUtils() {}

  /**
   * Close all resources related to the given class loader to prevent memory leaks.
   *
   * @param classLoader the classloader to be closed
   */
  public static void closeClassLoaderResource(ClassLoader classLoader) {
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;
    if (testEnv) {
      // In test environment, we do not need to clean up class loader related stuff
      return;
    }

    // Clear statics threads in FileSystem and close all FileSystem instances.
    executeAndCatch(
        ClassLoaderResourceCleanerUtils::closeStatsDataClearerInFileSystem, classLoader);

    // Stop all threads with the current class loader and clear their threadLocal variables for
    // jetty threads that are loaded by the current class loader.
    // For example, thread local `threadData` in FileSystem#StatisticsDataCleaner is created
    // within jetty thread with the current class loader. However, there are clear by
    // `catalog.close` in ForkJoinPool in CaffeineCache, in this case, the thread local variable
    // will not be cleared, so we need to clear them manually here.
    executeAndCatch(
        ClassLoaderResourceCleanerUtils::stopThreadsAndClearThreadLocalVariables, classLoader);

    // Release the LogFactory for the classloader, each classloader has its own LogFactory
    // instance.
    executeAndCatch(ClassLoaderResourceCleanerUtils::releaseLogFactoryInCommonLogging, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::removeLoggerContextListeners, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::deregisterJdbcDrivers, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::shutdownMysqlConnectionCleanup, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::removeSecurityProviders, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::clearResourceBundleCache, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::closeResourceInAWS, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::closeResourceInGCP, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::closeResourceInAzure, classLoader);

    executeAndCatch(ClassLoaderResourceCleanerUtils::clearShutdownHooks, classLoader);
  }

  /**
   * Close the stats data clearer thread in Hadoop FileSystem to prevent memory leaks when using
   *
   * @param targetClassLoader the classloader where Hadoop FileSystem is loaded
   */
  private static void closeStatsDataClearerInFileSystem(ClassLoader targetClassLoader)
      throws Exception {
    Class<?> fileSystemClass =
        Class.forName("org.apache.hadoop.fs.FileSystem", true, targetClassLoader);

    // If FileSystem was resolved from a parent/AppClassLoader rather than the catalog's own
    // classloader, its CACHE, Statistics cleaner, and MutableQuantiles scheduler are shared
    // across all catalogs in the JVM. Operating on shared static state here would close every
    // catalog's FileSystems and permanently terminate the global scheduler, breaking any
    // subsequent catalog that uses Hadoop metrics. Skip cleanup for shared classes and let
    // the JVM manage them.
    if (!isOwnedByClassLoader(fileSystemClass, targetClassLoader)) {
      LOG.debug(
          "Hadoop FileSystem is owned by {}, not the target classloader {}; skipping shared-class cleanup",
          fileSystemClass.getClassLoader(),
          targetClassLoader);
      return;
    }

    MethodUtils.invokeStaticMethod(fileSystemClass, "closeAll");

    Class<?> mutableQuantilesClass =
        Class.forName("org.apache.hadoop.metrics2.lib.MutableQuantiles", true, targetClassLoader);
    Class<?> statisticsClass =
        Class.forName("org.apache.hadoop.fs.FileSystem$Statistics", true, targetClassLoader);

    if (isOwnedByClassLoader(mutableQuantilesClass, targetClassLoader)) {
      ScheduledExecutorService scheduler =
          (ScheduledExecutorService)
              FieldUtils.readStaticField(mutableQuantilesClass, "scheduler", true);
      scheduler.shutdownNow();
    }

    if (isOwnedByClassLoader(statisticsClass, targetClassLoader)) {
      Field statisticsCleanerField =
          FieldUtils.getField(statisticsClass, "STATS_DATA_CLEANER", true);
      Object statisticsCleaner = statisticsCleanerField.get(null);
      if (statisticsCleaner != null) {
        ((Thread) statisticsCleaner).interrupt();
        ((Thread) statisticsCleaner).setContextClassLoader(null);
        ((Thread) statisticsCleaner).join();
      }
    }
  }

  /**
   * Stop all threads that are using the target class loader and clear thread local variables to
   * prevent memory leaks.
   *
   * <pre>
   * This method aims to:
   * 1. Stop all threads that are using the target class loader.
   * 2. Clear thread local variables in all threads that are using the target class loader. some thread
   * local variables are loaded in thread jetty-webserver-* threads, which are long-lived threads and
   * will not be stopped when the catalog is closed.
   * </pre>
   */
  private static void stopThreadsAndClearThreadLocalVariables(ClassLoader classLoader) {
    Thread[] threads = getAllThreads();
    for (Thread thread : threads) {
      // First clear thread local variables
      clearThreadLocalMap(thread, classLoader);
      // Close all threads that are using the FilesetCatalogOperations class loader
      if (runningWithClassLoader(thread, classLoader)) {
        LOG.debug("Interrupting thread: {}", thread.getName());
        thread.setContextClassLoader(null);
        thread.interrupt();
        try {
          thread.join(500);
        } catch (InterruptedException e) {
          LOG.debug("Failed to join thread: {}", thread.getName(), e);
        }
      }
    }
  }

  /**
   * Whether the thread belongs to the class loader being released.
   *
   * <p>The context ClassLoader is only one of the ways a thread can carry a catalog. A driver that
   * starts its own housekeeping thread, such as PostgreSQL's {@code LazyCleaner}, is running code
   * defined by the catalog's loader: the thread is a GC root, so its class alone keeps the loader
   * alive no matter what its context ClassLoader says.
   *
   * <p>Ownership has to be read from the thread itself, never from what it happens to be running: a
   * request thread executing an operation on this very catalog is not the catalog's to stop, and
   * interrupting it fails the request with "Thread was interrupted while waiting for lock".
   */
  @VisibleForTesting
  static boolean runningWithClassLoader(Thread thread, ClassLoader targetClassLoader) {
    if (thread == null) {
      return false;
    }
    if (thread.getContextClassLoader() == targetClassLoader
        || thread.getClass().getClassLoader() == targetClassLoader) {
      return true;
    }
    try {
      Object runnable = FieldUtils.readField(thread, "target", true);
      if (runnable != null && runnable.getClass().getClassLoader() == targetClassLoader) {
        return true;
      }
    } catch (Exception e) {
      LOG.debug("Cannot read the runnable of thread {}", thread.getName(), e);
    }

    return false;
  }

  private static Thread[] getAllThreads() {
    ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
    ThreadGroup parentGroup;
    while ((parentGroup = rootGroup.getParent()) != null) {
      rootGroup = parentGroup;
    }

    Thread[] threads = new Thread[rootGroup.activeCount()];
    while (rootGroup.enumerate(threads, true) == threads.length) {
      threads = new Thread[threads.length * 2];
    }
    return threads;
  }

  @VisibleForTesting
  static void clearThreadLocalMap(Thread thread, ClassLoader targetClassLoader) {
    if (thread == null) {
      return;
    }

    try {
      Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
      threadLocalsField.setAccessible(true);
      Object threadLocalMap = threadLocalsField.get(thread);

      if (threadLocalMap != null) {
        Class<?> tlmClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
        Field tableField = tlmClass.getDeclaredField("table");
        tableField.setAccessible(true);
        Object[] table = (Object[]) tableField.get(threadLocalMap);

        for (Object entry : table) {
          if (entry != null) {
            Object value = FieldUtils.readField(entry, "value", true);
            // The entry is a WeakReference to the ThreadLocal itself, which can be the leaking
            // side when the ThreadLocal was declared by a class of the dying catalog.
            Object key = entry instanceof Reference ? ((Reference<?>) entry).get() : null;
            if (definedBy(value, targetClassLoader) || definedBy(key, targetClassLoader)) {
              LOG.debug(
                  "Cleaning up thread local {} for thread {} with custom class loader",
                  value,
                  thread.getName());
              FieldUtils.writeField(entry, "value", null, true);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.debug("Failed to clean up thread locals for thread {}", thread.getName(), e);
    }
  }

  /**
   * Whether {@code value}, or what it refers to when it is a {@link Reference}, was defined by
   * {@code classLoader}.
   *
   * <p>Looking through a {@link Reference} matters: caches such as Jackson's {@code BufferRecycler}
   * park a {@code SoftReference} in a {@link ThreadLocal}. The reference itself is a bootstrap
   * class, so only its referent identifies the owning catalog. Left in place, such an entry keeps
   * the catalog's ClassLoader alive until heap pressure clears the soft reference, which Metaspace
   * pressure alone never triggers.
   */
  @VisibleForTesting
  static boolean definedBy(@Nullable Object value, ClassLoader classLoader) {
    if (value == null) {
      return false;
    }
    if (value.getClass().getClassLoader() == classLoader) {
      return true;
    }
    if (value instanceof Reference) {
      Object referent = ((Reference<?>) value).get();
      return referent != null && referent.getClass().getClassLoader() == classLoader;
    }
    return false;
  }

  /**
   * Clear shutdown hooks registered by the target class loader to prevent memory leaks.
   *
   * <p>All shutdown hooks are run with the system class loader, so we need to manually clear the
   * shutdown hooks registered by the target class loader.
   *
   * @param targetClassLoader the classloader where the shutdown hooks are registered.
   */
  private static void clearShutdownHooks(ClassLoader targetClassLoader) throws Exception {
    Class<?> shutdownHooks = Class.forName("java.lang.ApplicationShutdownHooks");
    IdentityHashMap<Thread, Thread> hooks =
        (IdentityHashMap<Thread, Thread>) FieldUtils.readStaticField(shutdownHooks, "hooks", true);

    hooks
        .entrySet()
        .removeIf(
            entry -> {
              Thread thread = entry.getKey();
              return thread.getContextClassLoader() == targetClassLoader;
            });
  }

  /**
   * Removes shutdown listeners the class loader registered on the shared Log4j {@code
   * LoggerContext}.
   *
   * <p>commons-logging's {@code Log4jApiLogFactory} registers a {@code LogAdapter} with the
   * LoggerContext of the server, which outlives every catalog. {@code LogFactory.release} drops the
   * factory from its own cache but leaves that registration in place, so the adapter's class, and
   * through it the catalog's ClassLoader, stays reachable from a static for the life of the
   * process.
   */
  /**
   * Drops the {@link ResourceBundle} cache entries loaded through this class loader.
   *
   * <p>{@link ResourceBundle} caches bundles in a JVM-wide static map, behind soft references. A
   * driver that loads message bundles, such as Oracle's {@code ErrorMessages}, therefore leaves its
   * class - and the catalog's ClassLoader - reachable until heap pressure clears the soft
   * reference, which Metaspace pressure alone never causes.
   */
  @VisibleForTesting
  static void clearResourceBundleCache(ClassLoader targetClassLoader) {
    ResourceBundle.clearCache(targetClassLoader);
  }

  /**
   * Removes the JCA security providers the class loader installed.
   *
   * <p>{@link Security} keeps installed providers in a JVM-wide static list. Hadoop's cloud
   * connectors install one, such as the shaded {@code OpenSSLProvider} that ships in the AWS
   * bundle, and it is never removed, so the provider's class holds the catalog's loader for the
   * life of the process.
   */
  @VisibleForTesting
  static void removeSecurityProviders(ClassLoader targetClassLoader) {
    for (Provider provider : Security.getProviders()) {
      if (provider.getClass().getClassLoader() == targetClassLoader) {
        Security.removeProvider(provider.getName());
        LOG.info("Removed security provider {} of a released catalog ClassLoader", provider);
      }
    }
  }

  /**
   * Shuts down MySQL Connector/J's abandoned-connection cleanup thread when the driver belongs to
   * this class loader.
   *
   * <p>The driver keeps that thread and its executor in a static field, and the executor's thread
   * factory is a lambda defined by the catalog's loader, so a running cleanup thread pins the
   * loader through its own stack frame. Connector/J exposes {@code uncheckedShutdown()} for exactly
   * this case.
   */
  private static void shutdownMysqlConnectionCleanup(ClassLoader targetClassLoader)
      throws Exception {
    Class<?> cleanupThreadClass =
        Class.forName(
            "com.mysql.cj.jdbc.AbandonedConnectionCleanupThread", true, targetClassLoader);
    if (!isOwnedByClassLoader(cleanupThreadClass, targetClassLoader)) {
      LOG.debug(
          "MySQL Connector/J is owned by {}, not {}; skipping shared-class cleanup",
          cleanupThreadClass.getClassLoader(),
          targetClassLoader);
      return;
    }
    // uncheckedShutdown stops the thread even when the driver still believes it is in use, which
    // is what unloading the ClassLoader requires; checkedShutdown returns without doing anything.
    MethodUtils.invokeStaticMethod(cleanupThreadClass, "uncheckedShutdown");
    LOG.info("Shut down the MySQL abandoned-connection cleanup thread of a released ClassLoader");
  }

  /**
   * Deregisters the JDBC drivers the class loader registered with {@link java.sql.DriverManager}.
   *
   * <p>{@code DriverManager} keeps registered drivers in a static list, and a driver defined by a
   * catalog's ClassLoader keeps that loader alive for the life of the process. It cannot be removed
   * from here directly: {@code DriverManager} filters both {@code getDrivers()} and {@code
   * deregisterDriver()} by the class loader of the calling class, so from the server's ClassLoader
   * the catalog's drivers are not even visible. Defining {@link JdbcDriverDeregisterer} inside the
   * target loader and calling it there gives {@code DriverManager} a caller that owns them.
   */
  @VisibleForTesting
  static void deregisterJdbcDrivers(ClassLoader targetClassLoader) throws Exception {
    String name = JdbcDriverDeregisterer.class.getName();
    byte[] bytecode;
    try (InputStream in =
        ClassLoaderResourceCleanerUtils.class
            .getClassLoader()
            .getResourceAsStream(name.replace('.', '/') + ".class")) {
      if (in == null) {
        LOG.debug("Cannot locate the bytecode of {}, skipping JDBC driver cleanup", name);
        return;
      }
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = in.read(chunk)) != -1) {
        buffer.write(chunk, 0, read);
      }
      bytecode = buffer.toByteArray();
    }

    Method defineClass =
        ClassLoader.class.getDeclaredMethod(
            "defineClass", String.class, byte[].class, int.class, int.class);
    defineClass.setAccessible(true);
    Class<?> deregisterer;
    try {
      deregisterer =
          (Class<?>) defineClass.invoke(targetClassLoader, name, bytecode, 0, bytecode.length);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof LinkageError) {
        // Already defined by an earlier cleanup of the same loader, whose drivers are gone.
        LOG.debug("{} is already defined in {}", name, targetClassLoader);
        return;
      }
      throw e;
    }

    Object deregistered = MethodUtils.invokeStaticMethod(deregisterer, "deregisterAll");
    if (deregistered instanceof Collection && !((Collection<?>) deregistered).isEmpty()) {
      LOG.info("Deregistered JDBC driver(s) {} of a released catalog ClassLoader", deregistered);
    }
  }

  @VisibleForTesting
  static void removeLoggerContextListeners(ClassLoader targetClassLoader) throws Exception {
    Class<?> logManagerClass = Class.forName("org.apache.logging.log4j.LogManager");
    Object contextFactory = MethodUtils.invokeStaticMethod(logManagerClass, "getFactory");
    Object selector = MethodUtils.invokeMethod(contextFactory, "getSelector");
    Collection<?> contexts =
        (Collection<?>) MethodUtils.invokeMethod(selector, "getLoggerContexts");
    for (Object context : contexts) {
      Collection<?> listeners = (Collection<?>) FieldUtils.readField(context, "listeners", true);
      if (listeners != null) {
        listeners.removeIf(
            listener ->
                listener != null && listener.getClass().getClassLoader() == targetClassLoader);
      }
    }
  }

  /**
   * Release the LogFactory for the target class loader to prevent memory leaks.
   *
   * @param currentClassLoader the classloader where the commons-logging is loaded.
   */
  private static void releaseLogFactoryInCommonLogging(ClassLoader currentClassLoader)
      throws Exception {

    // If we use fileset with the local file system, HTrace will be used, so we need to
    // release the HTrace LogFactory as well.
    try {
      Class<?> htraceLogFactoryClass =
          Class.forName(
              "org.apache.htrace.shaded.commons.logging.LogFactory", true, currentClassLoader);
      MethodUtils.invokeStaticMethod(htraceLogFactoryClass, "release", currentClassLoader);
    } catch (Exception e) {
      // Ignore if htrace is not used
      LOG.debug("HTrace is not used, skipping release of HTrace LogFactory...");
    }

    // Release the LogFactory for the FilesetCatalogOperations class loader
    Class<?> logFactoryClass =
        Class.forName("org.apache.commons.logging.LogFactory", true, currentClassLoader);
    MethodUtils.invokeStaticMethod(logFactoryClass, "release", currentClassLoader);
  }

  /**
   * Close the AWS SDK metrics MBean to prevent memory leaks when using AWS S3.
   *
   * @param classLoader the classloader where AWS SDK is loaded
   */
  private static void closeResourceInAWS(ClassLoader classLoader) throws Exception {
    Class<?> awsSdkMetricsClass =
        Class.forName("com.amazonaws.metrics.AwsSdkMetrics", true, classLoader);
    // AwsSdkMetrics holds a static MBeanServer registration. If the class was delegated to a
    // parent/AppClassLoader, unregistering here would remove the MBean for the entire JVM.
    if (!isOwnedByClassLoader(awsSdkMetricsClass, classLoader)) {
      LOG.debug(
          "AwsSdkMetrics is owned by {}, not {}; skipping MBean unregister",
          awsSdkMetricsClass.getClassLoader(),
          classLoader);
      return;
    }
    MethodUtils.invokeStaticMethod(awsSdkMetricsClass, "unregisterMetricAdminMBean");
  }

  private static void closeResourceInGCP(ClassLoader classLoader) throws Exception {
    Class<?> relocatedLogFactory =
        Class.forName(
            "org.apache.gravitino.gcp.shaded.org.apache.commons.logging.LogFactory",
            true,
            classLoader);
    // The GCP shaded LogFactory is always bundled inside the GCP plugin; if it resolves to a
    // different classloader, skip to avoid releasing a shared factory.
    if (!isOwnedByClassLoader(relocatedLogFactory, classLoader)) {
      LOG.debug(
          "GCP shaded LogFactory is owned by {}, not {}; skipping release",
          relocatedLogFactory.getClassLoader(),
          classLoader);
      return;
    }
    MethodUtils.invokeStaticMethod(relocatedLogFactory, "release", classLoader);
  }

  /**
   * Close the timer in AbfsClientThrottlingAnalyzer to prevent memory leaks when using Azure Blob
   * File System.
   *
   * <p>Timer is a daemon thread, so it won't prevent the JVM from shutting down, but it will
   * prevent the class loader from being garbage collected.
   *
   * @param classLoader the classloader where Azure Blob File System is loaded
   */
  private static void closeResourceInAzure(ClassLoader classLoader) throws Exception {
    Class<?> abfsClientThrottlingInterceptClass =
        Class.forName(
            "org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept",
            true,
            classLoader);
    // AbfsClientThrottlingIntercept holds a static singleton with Timers. If the ABFS class was
    // delegated to a parent/AppClassLoader, cancelling its timers would break ABFS for the JVM.
    if (!isOwnedByClassLoader(abfsClientThrottlingInterceptClass, classLoader)) {
      LOG.debug(
          "AbfsClientThrottlingIntercept is owned by {}, not {}; skipping Azure cleanup",
          abfsClientThrottlingInterceptClass.getClassLoader(),
          classLoader);
      return;
    }
    Object abfsClientThrottlingIntercept =
        FieldUtils.readStaticField(abfsClientThrottlingInterceptClass, "singleton", true);

    Object readThrottler =
        FieldUtils.readField(abfsClientThrottlingIntercept, "readThrottler", true);
    Object writeThrottler =
        FieldUtils.readField(abfsClientThrottlingIntercept, "writeThrottler", true);

    Timer readTimer = (Timer) FieldUtils.readField(readThrottler, "timer", true);
    readTimer.cancel();
    Timer writeTimer = (Timer) FieldUtils.readField(writeThrottler, "timer", true);
    writeTimer.cancel();

    Class<?> relocatedLogFactory =
        Class.forName(
            "org.apache.gravitino.azure.shaded.org.apache.commons.logging.LogFactory",
            true,
            classLoader);
    MethodUtils.invokeStaticMethod(relocatedLogFactory, "release", classLoader);
  }

  /**
   * Returns true if {@code clazz} was loaded directly by {@code classLoader} (not delegated to a
   * parent). Use this before touching static fields that must belong to the catalog's own
   * classloader to avoid accidentally mutating JVM-global shared state.
   */
  @VisibleForTesting
  static boolean isOwnedByClassLoader(Class<?> clazz, ClassLoader classLoader) {
    return classLoader != null && clazz.getClassLoader() == classLoader;
  }

  @FunctionalInterface
  private interface ThrowableConsumer<T> {
    void accept(T t) throws Exception;
  }

  private static <T> void executeAndCatch(ThrowableConsumer<T> consumer, T value) {
    try {
      consumer.accept(value);
    } catch (Exception e) {
      LOG.debug("Failed to execute consumer: ", e);
    }
  }
}
