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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.Security;
import org.junit.jupiter.api.Test;

class TestClassLoaderResourceCleanerUtils {

  private static final ThreadLocal<Object> SOFT_HOLDER = new ThreadLocal<>();
  private static final ThreadLocal<Object> UNRELATED_HOLDER = new ThreadLocal<>();

  /** A class with no dependencies beyond java.*, so a bare-bones child loader can define it. */
  public static class Leaky {}

  /** A Runnable the child loader can define, standing in for a driver's housekeeping task. */
  public static class LeakyTask implements Runnable {
    @Override
    public void run() {}
  }

  private static URLClassLoader childLoaderOwning(Class<?> clazz) throws Exception {
    URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
    // A null parent keeps delegation off the app loader, so the child defines the class itself.
    return new URLClassLoader(new URL[] {location}, null);
  }

  /** The value's own class identifies the owner in the simple case. */
  @Test
  void testDefinedByMatchesTheDeclaringLoader() throws Exception {
    try (URLClassLoader child = childLoaderOwning(Leaky.class)) {
      Object leaky = child.loadClass(Leaky.class.getName()).getDeclaredConstructor().newInstance();
      assertTrue(ClassLoaderResourceCleanerUtils.definedBy(leaky, child));
      assertFalse(ClassLoaderResourceCleanerUtils.definedBy(leaky, Leaky.class.getClassLoader()));
    }
  }

  /**
   * Caches such as Jackson's BufferRecycler park a SoftReference in a ThreadLocal. The reference is
   * a bootstrap class, so only its referent identifies the owning catalog.
   */
  @Test
  void testDefinedByLooksThroughAReference() throws Exception {
    try (URLClassLoader child = childLoaderOwning(Leaky.class)) {
      Object leaky = child.loadClass(Leaky.class.getName()).getDeclaredConstructor().newInstance();
      assertTrue(ClassLoaderResourceCleanerUtils.definedBy(new SoftReference<>(leaky), child));
      assertTrue(ClassLoaderResourceCleanerUtils.definedBy(new WeakReference<>(leaky), child));
    }
  }

  /** An empty reference names no owner and must not be mistaken for one. */
  @Test
  void testDefinedByIgnoresNullAndClearedReferences() {
    assertFalse(ClassLoaderResourceCleanerUtils.definedBy(null, getClass().getClassLoader()));
    assertFalse(
        ClassLoaderResourceCleanerUtils.definedBy(
            new SoftReference<>(null), getClass().getClassLoader()));
  }

  /**
   * A thread local holding the catalog's object behind a SoftReference must be cleared. Left in
   * place it keeps the catalog's ClassLoader alive until heap pressure clears the reference, which
   * Metaspace pressure alone never triggers.
   */
  @Test
  void testClearThreadLocalMapClearsSoftReferencedValues() throws Exception {
    try (URLClassLoader child = childLoaderOwning(Leaky.class)) {
      Object leaky = child.loadClass(Leaky.class.getName()).getDeclaredConstructor().newInstance();
      SOFT_HOLDER.set(new SoftReference<>(leaky));

      ClassLoaderResourceCleanerUtils.clearThreadLocalMap(Thread.currentThread(), child);

      assertNull(SOFT_HOLDER.get());
    }
  }

  /**
   * A driver's own housekeeping thread runs code the catalog defined, so the thread pins the loader
   * whatever its context ClassLoader says.
   */
  @Test
  void testRunningWithClassLoaderMatchesTheRunnableOfAThread() throws Exception {
    try (URLClassLoader child = childLoaderOwning(Leaky.class)) {
      Runnable owned =
          (Runnable)
              child.loadClass(LeakyTask.class.getName()).getDeclaredConstructor().newInstance();
      Thread thread = new Thread(owned, "leaky-task");
      thread.setContextClassLoader(null);

      assertTrue(ClassLoaderResourceCleanerUtils.runningWithClassLoader(thread, child));
      assertFalse(
          ClassLoaderResourceCleanerUtils.runningWithClassLoader(
              new Thread(() -> {}, "unrelated"), child));
    }
  }

  /** Providers installed by other loaders, and by the JDK itself, must be left alone. */
  @Test
  void testRemoveSecurityProvidersLeavesUnrelatedProviders() throws Exception {
    int before = Security.getProviders().length;
    try (URLClassLoader child = childLoaderOwning(Leaky.class)) {
      ClassLoaderResourceCleanerUtils.removeSecurityProviders(child);
    }
    assertEquals(before, Security.getProviders().length);
  }

  /** Entries belonging to another loader must survive the sweep. */
  @Test
  void testClearThreadLocalMapLeavesUnrelatedValues() throws Exception {
    try (URLClassLoader child = childLoaderOwning(Leaky.class)) {
      Object unrelated = new Object();
      UNRELATED_HOLDER.set(unrelated);

      ClassLoaderResourceCleanerUtils.clearThreadLocalMap(Thread.currentThread(), child);

      assertSame(unrelated, UNRELATED_HOLDER.get());
    }
  }

  /**
   * When a class is loaded by exactly the target classloader, isOwnedByClassLoader must return true
   * — the guard should allow static-state cleanup to proceed.
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
   * child), isOwnedByClassLoader must return false — the guard should skip cleanup to avoid
   * mutating shared JVM-global static state.
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
   * classloader — the guard must return false for them too.
   */
  @Test
  void testIsOwnedByClassLoaderReturnsFalseForBootstrapLoadedClass() {
    // String is loaded by the bootstrap classloader; getClassLoader() returns null.
    assertFalse(
        ClassLoaderResourceCleanerUtils.isOwnedByClassLoader(
            String.class, ClassLoader.getSystemClassLoader()));
  }
}
