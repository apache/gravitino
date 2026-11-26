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
package org.apache.gravitino.catalog.fileset;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.hadoop.util.ShutdownHookManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks, each in a JVM of its own, that the threads the catalog leaves to the JVM do not keep its
 * class loader: Hadoop's shutdown hook, which the class loader cleaner removes by its context class
 * loader, and the timer of the tasks on the pools.
 */
public class TestFilesetCatalogRelease {

  @ParameterizedTest
  @ValueSource(strings = {"hook", "timer"})
  public void testThreadsLeftToTheJvm(String check, @TempDir File dir) throws Exception {
    File log = new File(dir, "release-check.log");
    Process process =
        new ProcessBuilder(
                new File(System.getProperty("java.home"), "bin/java").getPath(),
                "--add-opens",
                "java.base/java.lang=ALL-UNNAMED",
                "-cp",
                System.getProperty("java.class.path"),
                ReleaseCheck.class.getName(),
                check)
            .redirectErrorStream(true)
            .redirectOutput(log)
            .start();
    try {
      boolean finished = process.waitFor(60, TimeUnit.SECONDS);
      String output = new String(Files.readAllBytes(log.toPath()), StandardCharsets.UTF_8);
      Assertions.assertTrue(finished, "the check did not finish: " + output);
      Assertions.assertEquals(0, process.exitValue(), output);
    } finally {
      process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Runs one check in a JVM where nothing has started Hadoop's shutdown hook manager or the JDK
   * timer yet.
   */
  public static final class ReleaseCheck {

    public static void main(String[] args) throws Exception {
      if ("hook".equals(args[0])) {
        checkHook();
      } else {
        checkTimer();
      }
    }

    // A catalog initialized with another context class loader than Hadoop's adds no hook, and one
    // initialized with Hadoop's adds the hook with that context class loader.
    private static void checkHook() throws Exception {
      Thread current = Thread.currentThread();
      ClassLoader hadoop = ShutdownHookManager.class.getClassLoader();
      current.setContextClassLoader(new ClassLoader(hadoop) {});
      try (FilesetCatalogOperations ops = initialized()) {
        check(hadoopHook() == null, "a hook with another context class loader");
      }
      current.setContextClassLoader(hadoop);
      try (FilesetCatalogOperations ops = initialized()) {
        Thread hook = hadoopHook();
        check(hook != null, "no hook");
        check(hook.getContextClassLoader() == hadoop, "the hook has " + hook);
      }
    }

    // The JVM-wide timer of CompletableFuture would keep the class loader of the first catalog that
    // starts it.
    private static void checkTimer() throws Exception {
      ThreadPoolExecutor pool =
          new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
      ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
      try {
        // Holds the task until start returns: orTimeout arms no timer for a future already done.
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<String> timed =
            FilesetCatalogOperations.TimedTask.start(
                pool,
                timer,
                () -> {
                  started.await();
                  return "done";
                },
                System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        started.countDown();
        String result = timed.get(30, TimeUnit.SECONDS);
        check("done".equals(result), "the task returned " + result);
      } finally {
        pool.shutdownNow();
        timer.shutdownNow();
      }
      for (Thread thread : Thread.getAllStackTraces().keySet()) {
        // CompletableFutureDelayScheduler, or ForkJoinPool.commonPool-delayScheduler from Java 25.
        check(
            !thread.getName().toLowerCase(Locale.ROOT).contains("delayscheduler"),
            "the JVM-wide timer is started: " + thread);
      }
    }

    private static FilesetCatalogOperations initialized() {
      FilesetCatalogOperations ops = new FilesetCatalogOperations(null, null);
      ops.initialize(
          Collections.emptyMap(),
          new CatalogInfo(
              1L,
              "catalog",
              CatalogInfo.Type.FILESET,
              "fileset",
              null,
              Collections.emptyMap(),
              null,
              Namespace.of("metalake")),
          new FilesetCatalogImpl());
      return ops;
    }

    @SuppressWarnings("unchecked")
    private static Thread hadoopHook() throws Exception {
      Class<?> hooks = Class.forName("java.lang.ApplicationShutdownHooks");
      Field field = hooks.getDeclaredField("hooks");
      field.setAccessible(true);
      synchronized (hooks) {
        for (Thread hook : ((Map<Thread, Thread>) field.get(null)).keySet()) {
          if (hook.getClass().getName().startsWith(ShutdownHookManager.class.getName() + "$")) {
            return hook;
          }
        }
      }
      return null;
    }

    private static void check(boolean condition, String message) {
      if (!condition) {
        throw new AssertionError(message);
      }
    }
  }
}
