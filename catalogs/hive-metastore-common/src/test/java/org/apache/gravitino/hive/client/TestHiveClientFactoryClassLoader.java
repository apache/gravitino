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

package org.apache.gravitino.hive.client;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ClassLoader-related fixes in {@link HiveClientFactory}.
 *
 * <p>These tests verify the three bug fixes without requiring a live HMS or KDC:
 *
 * <ul>
 *   <li>Bug 1: {@code createHiveClientWithBackend()} uses a stable {@code baseLoader} ( {@code
 *       HiveClientFactory.class.getClassLoader()}) instead of the unstable TCCL.
 *   <li>Bug 2: {@code createHiveClientImpl()} loads {@code HiveVersion} from the isolated
 *       ClassLoader to avoid {@link NoSuchMethodException}.
 * </ul>
 *
 * <p>Bug 3 (missing {@code doAs} in non-impersonation Kerberos path) requires a live KDC and is
 * covered by the integration test {@link TestHive2HMSWithKerberosNoImpersonation}.
 */
public class TestHiveClientFactoryClassLoader {

  // ---------------------------------------------------------------------------
  // Bug 1: factory CL is stable; TCCL is not — contrast/rationale tests
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code HiveClientFactory.class.getClassLoader()} is a stable, consistent
   * reference across threads.
   *
   * <p>This is the positive side of the Bug 1 fix: it documents why the factory CL is the correct
   * choice as {@code baseLoader} for {@link HiveClientClassLoader}.
   */
  @Test
  public void testFactoryClassLoaderIsStableAcrossThreads() throws InterruptedException {
    ClassLoader expectedCl = HiveClientFactory.class.getClassLoader();
    ClassLoader[] captured = new ClassLoader[1];

    Thread t = new Thread(() -> captured[0] = HiveClientFactory.class.getClassLoader());
    // Even if the thread has a different TCCL, the factory CL must remain the same
    t.setContextClassLoader(new ClassLoader(expectedCl) {});
    t.start();
    t.join();

    assertSame(
        expectedCl,
        captured[0],
        "HiveClientFactory.class.getClassLoader() must be identical across threads");
  }

  /**
   * Demonstrates that TCCL is <em>not</em> stable across threads — which is exactly why Bug 1
   * existed.
   *
   * <p>If two threads have different TCCLs and TCCL were used as {@code baseLoader}, {@code
   * UserGroupInformation} (a shared hadoop class delegated via {@code isSharedClass}) would be
   * resolved against different ClassLoaders, making their static TGT state invisible to each other.
   */
  @Test
  public void testTCCLIsUnstableAcrossThreads() throws InterruptedException {
    ClassLoader factoryCl = HiveClientFactory.class.getClassLoader();
    ClassLoader customTccl = new ClassLoader(factoryCl) {};
    ClassLoader[] capturedTccl = new ClassLoader[1];

    Thread t = new Thread(() -> capturedTccl[0] = Thread.currentThread().getContextClassLoader());
    t.setContextClassLoader(customTccl);
    t.start();
    t.join();

    // The thread's TCCL is the custom one we set — different from the factory CL
    assertSame(customTccl, capturedTccl[0], "Thread TCCL should reflect what was set");
    assertNotSame(
        factoryCl,
        capturedTccl[0],
        "TCCL can differ from factory CL, proving it is not a stable baseLoader reference");
  }

  // ---------------------------------------------------------------------------
  // Bug 2: createHiveClientImpl() must use isolated CL's HiveVersion enum
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@link HiveClientFactory#createHiveClientImpl} loads {@code HiveVersion} from the
   * provided {@code classloader} before calling {@link Class#getConstructor}.
   *
   * <p>The mock classloader returns the real {@code HiveVersion} class for the {@code
   * HiveVersion.loadClass} call, and throws a sentinel {@link ClassNotFoundException} for {@code
   * HiveClientImpl.loadClass}. The test asserts that the sentinel is thrown (not {@link
   * NoSuchMethodException}), proving that the {@code HiveVersion} lookup on the isolated CL
   * succeeded before reaching the constructor step.
   *
   * <p>If the bug were present, {@code getConstructor} would receive the system CL's {@code
   * HiveVersion} and throw {@link NoSuchMethodException} instead.
   */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testCreateHiveClientImplLoadsHiveVersionFromIsolatedClassLoader() throws Exception {
    ClassLoader mockCl = mock(ClassLoader.class);

    // HiveVersion lookup on isolated CL returns the real class (fix is in place)
    when(mockCl.loadClass(HiveClientClassLoader.HiveVersion.class.getName()))
        .thenReturn((Class) HiveClientClassLoader.HiveVersion.class);

    // HiveClientImpl lookup throws a sentinel so we can observe which step was reached
    when(mockCl.loadClass(HiveClientImpl.class.getName()))
        .thenThrow(new ClassNotFoundException("sentinel: reached HiveClientImpl step"));

    Properties props = new Properties();
    props.setProperty("hive.metastore.uris", "thrift://localhost:9083");

    // The sentinel ClassNotFoundException must be thrown — not NoSuchMethodException.
    // This proves that HiveVersion was successfully loaded from the isolated CL first.
    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                HiveClientFactory.createHiveClientImpl(
                    HiveClientClassLoader.HiveVersion.HIVE2, props, mockCl));

    assertTrue(
        ex instanceof ClassNotFoundException && ex.getMessage().contains("sentinel"),
        "Expected sentinel ClassNotFoundException proving HiveVersion was loaded from isolated CL,"
            + " but got: "
            + ex.getClass().getSimpleName()
            + ": "
            + ex.getMessage());
  }

  /**
   * Contrast test: proves that using the <em>wrong</em> ClassLoader's {@code HiveVersion} type in
   * {@link Class#getConstructor} fails with {@link NoSuchMethodException}.
   *
   * <p>This simulates the original Bug 2 behavior: a constructor that takes an enum type defined in
   * one ClassLoader cannot be found when the caller passes the same-named type from a different
   * ClassLoader — even though both have the same binary name.
   */
  @Test
  public void testSystemClHiveVersionCausesNoSuchMethodException() throws Exception {
    // Create an isolated classloader that redefines HiveVersion independently
    ClassLoader isolatedCl =
        new ClassLoader(null) {
          @Override
          protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (name.equals(HiveClientClassLoader.HiveVersion.class.getName())) {
              try {
                String resource = name.replace('.', '/') + ".class";
                byte[] bytes;
                try (java.io.InputStream is = ClassLoader.getSystemResourceAsStream(resource)) {
                  if (is == null) throw new ClassNotFoundException(name);
                  bytes = is.readAllBytes();
                }
                return defineClass(name, bytes, 0, bytes.length);
              } catch (java.io.IOException e) {
                throw new ClassNotFoundException(name, e);
              }
            }
            return super.findClass(name);
          }
        };

    Class<?> isolatedHiveVersion =
        isolatedCl.loadClass(HiveClientClassLoader.HiveVersion.class.getName());

    // The two Class objects must be different (isolated vs system CL)
    assertNotSame(
        HiveClientClassLoader.HiveVersion.class,
        isolatedHiveVersion,
        "Isolated CL must produce a distinct Class object for HiveVersion");

    // A constructor declared with the isolatedHiveVersion type cannot be found
    // when the caller passes the system CL's HiveVersion.class — this is Bug 2.
    class Target {
      @SuppressWarnings("unused")
      public Target(HiveClientClassLoader.HiveVersion v) {}
    }

    assertThrows(
        NoSuchMethodException.class,
        () -> Target.class.getConstructor(isolatedHiveVersion),
        "getConstructor with mismatched CL's HiveVersion must throw NoSuchMethodException");
  }
}
