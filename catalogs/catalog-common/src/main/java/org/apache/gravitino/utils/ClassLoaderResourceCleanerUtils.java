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

import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.Timer;
import java.util.concurrent.ScheduledExecutorService;
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
    MethodUtils.invokeStaticMethod(fileSystemClass, "closeAll");

    Class<?> mutableQuantilesClass =
        Class.forName("org.apache.hadoop.metrics2.lib.MutableQuantiles", true, targetClassLoader);
    Class<?> statisticsClass =
        Class.forName("org.apache.hadoop.fs.FileSystem$Statistics", true, targetClassLoader);

    ScheduledExecutorService scheduler =
        (ScheduledExecutorService)
            FieldUtils.readStaticField(mutableQuantilesClass, "scheduler", true);
    scheduler.shutdownNow();
    Field statisticsCleanerField = FieldUtils.getField(statisticsClass, "STATS_DATA_CLEANER", true);
    Object statisticsCleaner = statisticsCleanerField.get(null);
    if (statisticsCleaner != null) {
      ((Thread) statisticsCleaner).interrupt();
      ((Thread) statisticsCleaner).setContextClassLoader(null);
      ((Thread) statisticsCleaner).join();
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
        LOG.info("Interrupting thread: {}", thread.getName());
        thread.setContextClassLoader(null);
        thread.interrupt();
        try {
          thread.join(500);
        } catch (InterruptedException e) {
          LOG.warn("Failed to join thread: {}", thread.getName(), e);
        }
      }
    }
  }

  private static boolean runningWithClassLoader(Thread thread, ClassLoader targetClassLoader) {
    return thread != null && thread.getContextClassLoader() == targetClassLoader;
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

  private static void clearThreadLocalMap(Thread thread, ClassLoader targetClassLoader) {
    if (thread == null || !thread.getName().startsWith("Gravitino-webserver-")) {
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
            if (value != null
                && value.getClass().getClassLoader() != null
                && value.getClass().getClassLoader() == targetClassLoader) {
              LOG.info(
                  "Cleaning up thread local {} for thread {} with custom class loader",
                  value,
                  thread.getName());
              FieldUtils.writeField(entry, "value", null, true);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to clean up thread locals for thread {}", thread.getName(), e);
    }
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
   * Release the LogFactory for the target class loader to prevent memory leaks.
   *
   * @param currentClassLoader the classloader where the commons-logging is loaded.
   */
  private static void releaseLogFactoryInCommonLogging(ClassLoader currentClassLoader)
      throws Exception {
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
    // For Aws SDK metrics, unregister the metric admin MBean
    Class<?> awsSdkMetricsClass =
        Class.forName("com.amazonaws.metrics.AwsSdkMetrics", true, classLoader);
    MethodUtils.invokeStaticMethod(awsSdkMetricsClass, "unregisterMetricAdminMBean");
  }

  private static void closeResourceInGCP(ClassLoader classLoader) throws Exception {
    // For GCS
    Class<?> relocatedLogFactory =
        Class.forName(
            "org.apache.gravitino.gcp.shaded.org.apache.commons.logging.LogFactory",
            true,
            classLoader);
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
    // Clear timer in AbfsClientThrottlingAnalyzer
    Class<?> abfsClientThrottlingInterceptClass =
        Class.forName(
            "org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept",
            true,
            classLoader);
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

    // Release the LogFactory for the Azure shaded commons logging which has been relocated
    // by the Azure SDK
    Class<?> relocatedLogFactory =
        Class.forName(
            "org.apache.gravitino.azure.shaded.org.apache.commons.logging.LogFactory",
            true,
            classLoader);
    MethodUtils.invokeStaticMethod(relocatedLogFactory, "release", classLoader);
  }

  @FunctionalInterface
  private interface ThrowableConsumer<T> {
    void accept(T t) throws Exception;
  }

  private static <T> void executeAndCatch(ThrowableConsumer<T> consumer, T value) {
    try {
      consumer.accept(value);
    } catch (Exception e) {
      LOG.warn("Failed to execute consumer: ", e);
    }
  }
}
