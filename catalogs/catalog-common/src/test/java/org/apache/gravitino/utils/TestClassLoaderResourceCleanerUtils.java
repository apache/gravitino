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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class TestClassLoaderResourceCleanerUtils {

  /** A trivial payload whose only purpose is to be loaded by a child ClassLoader in tests. */
  public static class LeakyValue {}

  /**
   * When a class is loaded by exactly the target classloader, isOwnedByClassLoader must return
   * true: the guard should allow static-state cleanup to proceed.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsTrueForOwningLoader() {
    ClassLoader loader = ClassLoaderResourceCleanerUtils.class.getClassLoader();
    assertTrue(
        ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
            ClassLoaderResourceCleanerUtils.class, loader));
  }

  /**
   * When a class was resolved via parent delegation (i.e. the actual loader is the parent, not the
   * child), isOwnedByClassLoader must return false: the guard should skip cleanup to avoid mutating
   * shared JVM-global static state.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsFalseForParentDelegatedClass() throws Exception {
    ClassLoader parent = ClassLoaderResourceCleanerUtils.class.getClassLoader();
    // Child delegates everything to the parent; ClassLoaderResourceCleanerUtils is therefore
    // parent-loaded, not child-loaded.
    try (URLClassLoader child = new URLClassLoader(new URL[0], parent)) {
      assertFalse(
          ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
              ClassLoaderResourceCleanerUtils.class, child));
    }
  }

  /**
   * Bootstrap-loaded classes (whose getClassLoader() returns null) are never "owned" by a named
   * classloader, so the guard must return false for them too.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsFalseForBootstrapLoadedClass() {
    // String is loaded by the bootstrap classloader; getClassLoader() returns null.
    assertFalse(
        ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
            String.class, ClassLoader.getSystemClassLoader()));
  }

  /**
   * Loading MySQL's {@code AbandonedConnectionCleanupThread} starts a daemon thread {@code
   * mysql-cj-abandoned-connection-cleanup} whose contextClassLoader is the loader that loaded it.
   * That reference pins the ClassLoader and leaks Metaspace after a catalog is dropped. {@code
   * shutdownMySQLAbandonedConnectionCleanupThread} must terminate that thread when the class is
   * owned by the target loader.
   */
  @Test
  void testShutdownMySQLAbandonedConnectionCleanupThreadStopsThread() throws Exception {
    URL mysqlJar = mysqlConnectorJarUrl();
    Assumptions.assumeTrue(mysqlJar != null, "mysql-connector jar is not on the test classpath");

    // Parent is the platform loader so `child` loads its OWN copy of the class (no delegation to
    // the app loader): the cleanup thread's contextClassLoader is `child` and the ownership guard
    // passes. Load AbandonedConnectionCleanupThread directly rather than the Driver so the thread
    // starts without registering a child-loaded Driver into the JVM-global DriverManager. That
    // registration would pin `child` after this test and leak it, which is the leak this utility
    // exists to prevent.
    ClassLoader platform = ClassLoader.getSystemClassLoader().getParent();
    try (URLClassLoader child = new URLClassLoader(new URL[] {mysqlJar}, platform)) {
      Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread", true, child);
      assertNotNull(
          findCleanupThreadBoundTo(child),
          "loading AbandonedConnectionCleanupThread should start the cleanup thread bound to child");

      ClassLoaderResourceCleanerUtils.shutdownMySQLAbandonedConnectionCleanupThread(child);

      assertTrue(
          waitForCleanupThreadGone(child),
          "cleanup thread bound to the child loader should be gone after uncheckedShutdown");
    }
  }

  /**
   * When the MySQL class resolves to a parent/shared ClassLoader (the driver sits on a shared
   * classpath while the catalog being closed has its own child loader), {@code
   * shutdownMySQLAbandonedConnectionCleanupThread} must skip the JVM-global {@code
   * uncheckedShutdown()} so closing one catalog does not stop a cleanup thread owned by another
   * loader.
   */
  @Test
  void testShutdownMySQLAbandonedConnectionCleanupThreadSkipsParentOwnedClass() throws Exception {
    URL mysqlJar = mysqlConnectorJarUrl();
    Assumptions.assumeTrue(mysqlJar != null, "mysql-connector jar is not on the test classpath");

    ClassLoader platform = ClassLoader.getSystemClassLoader().getParent();
    // `owner` holds the class; `child` delegates to it, mirroring a driver on a shared parent CL.
    try (URLClassLoader owner = new URLClassLoader(new URL[] {mysqlJar}, platform);
        URLClassLoader child = new URLClassLoader(new URL[0], owner)) {
      Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread", true, owner);
      assertNotNull(
          findCleanupThreadBoundTo(owner), "cleanup thread should be bound to the owner loader");

      // classLoader=child, but the class resolves to `owner` via delegation, so the guard skips.
      ClassLoaderResourceCleanerUtils.shutdownMySQLAbandonedConnectionCleanupThread(child);
      // When uncheckedShutdown() does run it kills the thread within ~100ms (see the positive
      // test's poll). Wait past that window and confirm the owner's thread is still alive, which
      // shows the guard skipped it rather than just not having stopped it yet. Without the guard,
      // 1s is long enough for the shutdown to take effect and this assertion fails.
      assertTrue(
          cleanupThreadStaysAliveFor(owner, 1000),
          "cleanup thread owned by the parent loader must survive a child-scoped cleanup");

      // An owner-scoped cleanup does run (guard passes), so nothing lingers past this test.
      ClassLoaderResourceCleanerUtils.shutdownMySQLAbandonedConnectionCleanupThread(owner);
      assertTrue(
          waitForCleanupThreadGone(owner), "owner-scoped cleanup should stop the thread it owns");
    }
  }

  /**
   * The broadened {@code clearThreadLocalMap} must clear a ThreadLocal whose value is owned by the
   * target ClassLoader even on a non-{@code Gravitino-webserver-*} thread (the pre-#10093 code only
   * touched webserver threads). A ThreadLocal holding a value from a different ClassLoader must be
   * left untouched, which is the {@code isOwnedByClassLoader}-style guard inside the sweep.
   */
  @Test
  @SuppressWarnings("ThreadLocalUsage") // local ThreadLocals are required to test per-thread sweep
  void testClearThreadLocalMapClearsOnlyTargetClassLoaderValues() throws Exception {
    URL classesUrl = LeakyValue.class.getProtectionDomain().getCodeSource().getLocation();
    ClassLoader platform = ClassLoader.getSystemClassLoader().getParent();
    ThreadLocal<Object> targetOwned = new ThreadLocal<>();
    ThreadLocal<Object> foreign = new ThreadLocal<>();
    // `child` is the cleanup target; `sibling` is an unrelated loader. Both load their own copy of
    // LeakyValue, so both values have a non-null ClassLoader. This exercises the real
    // discrimination (value CL == target vs a different non-null CL), not just the null-CL case.
    try (URLClassLoader child = new URLClassLoader(new URL[] {classesUrl}, platform);
        URLClassLoader sibling = new URLClassLoader(new URL[] {classesUrl}, platform)) {
      Object childOwnedValue =
          Class.forName(LeakyValue.class.getName(), true, child)
              .getDeclaredConstructor()
              .newInstance();
      assertSame(child, childOwnedValue.getClass().getClassLoader());
      Object siblingOwnedValue =
          Class.forName(LeakyValue.class.getName(), true, sibling)
              .getDeclaredConstructor()
              .newInstance();
      assertSame(sibling, siblingOwnedValue.getClass().getClassLoader());

      // This test runs on the JUnit "main"/worker thread, deliberately NOT a webserver thread, so
      // it also proves the broadened scope.
      assertFalse(Thread.currentThread().getName().startsWith("Gravitino-webserver-"));

      targetOwned.set(childOwnedValue);
      foreign.set(siblingOwnedValue);

      ClassLoaderResourceCleanerUtils.clearThreadLocalMap(Thread.currentThread(), child);

      assertNull(targetOwned.get(), "ThreadLocal owned by the target ClassLoader must be cleared");
      assertSame(
          siblingOwnedValue,
          foreign.get(),
          "ThreadLocal owned by a different non-null ClassLoader must be kept");
    } finally {
      targetOwned.remove();
      foreign.remove();
    }
  }

  /**
   * JVM-internal threads live in the {@code system} thread group; the sweep must skip them entirely
   * rather than reflect into their ThreadLocals.
   */
  @Test
  @SuppressWarnings("ThreadLocalUsage") // local ThreadLocal is required to test per-thread sweep
  void testClearThreadLocalMapSkipsSystemThreadGroup() throws Exception {
    ClassLoader loader = ClassLoaderResourceCleanerUtils.class.getClassLoader();
    ThreadGroup systemGroup = systemThreadGroup();
    assertNotNull(systemGroup, "expected to locate the system thread group");

    Object[] cleared = new Object[1];
    Thread systemThread =
        new Thread(
            systemGroup,
            () -> {
              ThreadLocal<Object> tl = new ThreadLocal<>();
              tl.set(new LeakyValue());
              // Sweeping a system-group thread must be a no-op: the value stays set.
              ClassLoaderResourceCleanerUtils.clearThreadLocalMap(Thread.currentThread(), loader);
              cleared[0] = tl.get();
            },
            "clp-system-group-probe");
    systemThread.start();
    systemThread.join();

    assertNotNull(cleared[0], "system-group thread ThreadLocals must be left untouched");
  }

  /**
   * End-to-end proof that broadening the sweep lets a dropped catalog's ClassLoader be collected. A
   * long-lived worker thread that is not a {@code Gravitino-webserver-*} thread holds a ThreadLocal
   * whose value was loaded by the target ClassLoader, which is exactly what pins the ClassLoader
   * after a catalog is dropped. After {@code clearThreadLocalMap} runs and the test drops its own
   * references, a WeakReference to the ClassLoader must clear once GC runs. The webserver-only
   * filter used before would skip this thread and the ClassLoader would stay reachable.
   */
  @Test
  @SuppressWarnings("ThreadLocalUsage") // a per-thread ThreadLocal is the leak this test reproduces
  void testClearThreadLocalMapLetsDroppedClassLoaderBeCollected() throws Exception {
    URL classesUrl = LeakyValue.class.getProtectionDomain().getCodeSource().getLocation();
    ClassLoader platform = ClassLoader.getSystemClassLoader().getParent();

    WeakReference<ClassLoader> ref;
    // Blocks the worker after it sets the ThreadLocal so the thread stays alive; a thread that
    // exits would null its own threadLocals in Thread.exit() and hide the leak.
    CountDownLatch release = new CountDownLatch(1);
    Thread worker = null;
    try {
      URLClassLoader child = new URLClassLoader(new URL[] {classesUrl}, platform);
      Object childOwnedValue =
          Class.forName(LeakyValue.class.getName(), true, child)
              .getDeclaredConstructor()
              .newInstance();
      // Guard the test's premise: the value must be loaded by `child`, otherwise the sweep's
      // value-ClassLoader check never matches and the reclamation below would prove nothing.
      assertSame(child, childOwnedValue.getClass().getClassLoader());
      ref = new WeakReference<>(child);

      CountDownLatch valueSet = new CountDownLatch(1);
      Object[] valueHolder = {childOwnedValue};
      worker =
          new Thread(
              () -> {
                ThreadLocal<Object> tl = new ThreadLocal<>();
                tl.set(valueHolder[0]);
                valueSet.countDown();
                try {
                  release.await();
                } catch (InterruptedException ignored) {
                  // test teardown
                }
              },
              "clp-reclaim-probe");
      worker.setDaemon(true);
      worker.start();
      assertTrue(valueSet.await(10, TimeUnit.SECONDS), "worker should set its ThreadLocal");

      ClassLoaderResourceCleanerUtils.clearThreadLocalMap(worker, child);

      // Drop every strong reference the test holds; the only thing that could still pin `child` is
      // the worker's ThreadLocal, which the sweep just cleared.
      child.close();
      child = null;
      childOwnedValue = null;
      valueHolder[0] = null;

      // Assert before releasing the worker: once it returns, Thread.exit() nulls its threadLocals
      // and would let `child` be collected even if the sweep did nothing.
      assertTrue(
          awaitCollected(ref),
          "dropped ClassLoader must be collectable once its ThreadLocal is swept");
    } finally {
      release.countDown();
      if (worker != null) {
        worker.join(1000);
      }
    }
  }

  private static boolean awaitCollected(WeakReference<?> ref) throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      if (ref.get() == null) {
        return true;
      }
      System.gc();
      Thread.sleep(50);
    }
    return ref.get() == null;
  }

  private static URL mysqlConnectorJarUrl() {
    // The driver class is on the test classpath (declared as testImplementation). Resolve its
    // code-source URL so a child URLClassLoader can load an isolated copy of the driver.
    try {
      Class<?> driver = Class.forName("com.mysql.cj.jdbc.Driver");
      return driver.getProtectionDomain().getCodeSource().getLocation();
    } catch (Throwable t) {
      return null;
    }
  }

  private static Thread findCleanupThreadBoundTo(ClassLoader loader) {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains("abandoned-connection-cleanup")
          && t.getContextClassLoader() == loader) {
        return t;
      }
    }
    return null;
  }

  private static boolean waitForCleanupThreadGone(ClassLoader loader) throws InterruptedException {
    for (int i = 0; i < 50; i++) {
      if (findCleanupThreadBoundTo(loader) == null) {
        return true;
      }
      Thread.sleep(100);
    }
    return findCleanupThreadBoundTo(loader) == null;
  }

  private static boolean cleanupThreadStaysAliveFor(ClassLoader loader, long millis)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + millis;
    while (System.currentTimeMillis() < deadline) {
      if (findCleanupThreadBoundTo(loader) == null) {
        return false;
      }
      Thread.sleep(50);
    }
    return findCleanupThreadBoundTo(loader) != null;
  }

  private static ThreadGroup systemThreadGroup() {
    ThreadGroup group = Thread.currentThread().getThreadGroup();
    while (group != null) {
      if ("system".equals(group.getName())) {
        return group;
      }
      group = group.getParent();
    }
    return null;
  }
}
