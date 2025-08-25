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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassLoaderUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderUtils.class);

  private ClassLoaderUtils() {}

  /**
   * Close the stats data clearer thread in Hadoop FileSystem to prevent memory leaks when using
   *
   * @param targetClassLoader the classloader where Hadoop FileSystem is loaded
   */
  public static void closeStatsDataClearerInFileSystem(ClassLoader targetClassLoader) {
    try {
      Class<?> FileSystem =
          Class.forName("org.apache.hadoop.fs.FileSystem", true, targetClassLoader);
      MethodUtils.invokeStaticMethod(FileSystem, "closeAll");

      Class<?> MutableQuantiles =
          Class.forName("org.apache.hadoop.metrics2.lib.MutableQuantiles", true, targetClassLoader);
      Class<?> statisticsClass =
          Class.forName("org.apache.hadoop.fs.FileSystem$Statistics", true, targetClassLoader);

      ScheduledExecutorService scheduler =
          (ScheduledExecutorService)
              FieldUtils.readStaticField(MutableQuantiles, "scheduler", true);
      scheduler.shutdownNow();
      Field statisticsCleanerField =
          FieldUtils.getField(statisticsClass, "STATS_DATA_CLEANER", true);
      Object statisticsCleaner = statisticsCleanerField.get(null);
      if (statisticsCleaner != null) {
        ((Thread) statisticsCleaner).interrupt();
        ((Thread) statisticsCleaner).setContextClassLoader(null);
        ((Thread) statisticsCleaner).join();
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to close FileSystem and MutableQuantiles statistics cleaner in IcebergCatalogOperations",
          e);
    }
  }

  public static Thread[] getAllThreads() {
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

  public static void clearThreadLocalMap(Thread thread, ClassLoader targetClassLoader) {
    if (thread != null && thread.getName().startsWith("Gravitino-webserver-")) {
      // Try to
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
  }

  public static void clearThreadPoolInHiveCatalog(ClassLoader classLoader) {
    try {
      Class<?> cachedClientPool =
          Class.forName("org.apache.iceberg.hive.CachedClientPool", true, classLoader);
      Object clientPoolCache =
          FieldUtils.readStaticField(cachedClientPool, "clientPoolCache", true);

      Object cache = FieldUtils.readField(clientPoolCache, "cache", true);

      Object scheduler =
          FieldUtils.readField(
              FieldUtils.readField(FieldUtils.readField(cache, "pacer", true), "scheduler", true),
              "delegate",
              true);
      ScheduledExecutorService executorService =
          (ScheduledExecutorService)
              FieldUtils.readField(scheduler, "scheduledExecutorService", true);
      executorService.shutdownNow();

      // Interrupt the thread if it is still running
    } catch (Exception e) {
      LOG.warn("Failed to clear Hive Metastore Cleaner", e);
    }
  }

  public static boolean runningWithClassLoader(Thread thread, ClassLoader targetClassLoader) {
    return thread != null && thread.getContextClassLoader() == targetClassLoader;
  }

  public static void clearShutdownHooks(ClassLoader targetClassLoader) {
    try {
      Class<?> shutdownHooks = Class.forName("java.lang.ApplicationShutdownHooks");
      IdentityHashMap<Thread, Thread> hooks =
          (IdentityHashMap<Thread, Thread>)
              FieldUtils.readStaticField(shutdownHooks, "hooks", true);

      hooks
          .entrySet()
          .removeIf(
              entry -> {
                Thread thread = entry.getKey();
                return thread.getContextClassLoader() == targetClassLoader;
              });
    } catch (Exception e) {
      LOG.warn("Failed to clean shutdown hooks", e);
    }
  }

  public static void releaseLogFactoryInCommonLogging(ClassLoader currentClassLoader) {
    // Release the LogFactory for the FilesetCatalogOperations class loader
    try {
      Class<?> logFactoryClass =
          Class.forName("org.apache.commons.logging.LogFactory", true, currentClassLoader);
      MethodUtils.invokeStaticMethod(logFactoryClass, "release", currentClassLoader);
    } catch (Exception e) {
      LOG.warn("Failed to release LogFactory for IcebergCatalogOperations class loader", e);
    }
  }
}
