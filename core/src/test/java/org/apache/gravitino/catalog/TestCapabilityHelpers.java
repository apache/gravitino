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

package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CapabilityHelpers#getCapability}.
 *
 * <p>Verifies that the returned {@link Capability} is a JDK dynamic proxy that correctly switches
 * the thread context classloader (TCCL) to the catalog's {@link IsolatedClassLoader} for every
 * method invocation, and restores the original TCCL afterwards.
 *
 * <p>This guards against {@link NoClassDefFoundError} caused by compiler-generated synthetic
 * classes (e.g. the anonymous {@code $1} class produced by {@code switch-on-enum}) being requested
 * from the server classloader, which cannot find them, when a {@link Capability} method is invoked
 * outside {@code withClassLoader()}.
 */
public class TestCapabilityHelpers {

  private IsolatedClassLoader isolatedClassLoader;
  private Capability delegate;
  private CatalogManager.CatalogWrapper wrapper;
  private CatalogManager catalogManager;

  @BeforeEach
  public void setUp() throws Exception {
    isolatedClassLoader =
        new IsolatedClassLoader(
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList());
    delegate = mock(Capability.class);
    when(delegate.caseSensitiveOnName(Capability.Scope.SCHEMA))
        .thenReturn(CapabilityResult.unsupported("Hive is case insensitive."));
    when(delegate.caseSensitiveOnName(Capability.Scope.TABLE))
        .thenReturn(CapabilityResult.unsupported("Hive is case insensitive."));
    when(delegate.managedStorage(Capability.Scope.SCHEMA)).thenReturn(CapabilityResult.SUPPORTED);

    wrapper = mock(CatalogManager.CatalogWrapper.class);
    when(wrapper.capabilities()).thenReturn(delegate);
    when(wrapper.classLoader()).thenReturn(isolatedClassLoader);

    NameIdentifier catalogIdent = NameIdentifier.of("metalake", "catalog");
    catalogManager = mock(CatalogManager.class);
    when(catalogManager.loadCatalogAndWrap(catalogIdent)).thenReturn(wrapper);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (isolatedClassLoader != null) {
      isolatedClassLoader.close();
      isolatedClassLoader = null;
    }
  }

  @Test
  public void testGetCapabilityReturnsProxy() throws Exception {
    Capability capability =
        CapabilityHelpers.getCapability(
            NameIdentifier.of("metalake", "catalog", "schema"), catalogManager);

    assertNotNull(capability);
    assertTrue(
        Proxy.isProxyClass(capability.getClass()),
        "getCapability() must return a JDK dynamic proxy");
  }

  @Test
  public void testProxyExecutesMethodWithinCatalogClassloader() throws Exception {
    AtomicReference<ClassLoader> capturedClassLoader = new AtomicReference<>();
    ClassLoader testClassLoader = Thread.currentThread().getContextClassLoader();
    Capability recordingDelegate =
        new Capability() {
          @Override
          public CapabilityResult caseSensitiveOnName(Scope scope) {
            capturedClassLoader.set(Thread.currentThread().getContextClassLoader());
            return CapabilityResult.SUPPORTED;
          }
        };
    when(wrapper.capabilities()).thenReturn(recordingDelegate);

    Capability proxy =
        CapabilityHelpers.getCapability(
            NameIdentifier.of("metalake", "catalog", "schema"), catalogManager);
    proxy.caseSensitiveOnName(Capability.Scope.SCHEMA);

    assertNotNull(capturedClassLoader.get(), "TCCL must have been set inside the proxy method");
    assertNotSame(
        testClassLoader,
        capturedClassLoader.get(),
        "TCCL inside the proxy must differ from the caller's classloader");
    assertTrue(
        capturedClassLoader.get().getClass().getName().contains("IsolatedClassLoader"),
        "TCCL inside the proxy must be the catalog's isolated classloader");
  }

  @Test
  public void testProxyRestoresTcclAfterMethodCall() throws Exception {
    ClassLoader testClassLoader = Thread.currentThread().getContextClassLoader();

    Capability proxy =
        CapabilityHelpers.getCapability(
            NameIdentifier.of("metalake", "catalog", "schema"), catalogManager);
    proxy.caseSensitiveOnName(Capability.Scope.SCHEMA);

    assertSame(
        testClassLoader,
        Thread.currentThread().getContextClassLoader(),
        "TCCL must be restored to its original value after the proxy method returns");
  }

  @Test
  public void testProxyPropagatesReturnValue() throws Exception {
    Capability proxy =
        CapabilityHelpers.getCapability(
            NameIdentifier.of("metalake", "catalog", "schema"), catalogManager);

    CapabilityResult result = proxy.caseSensitiveOnName(Capability.Scope.SCHEMA);

    assertEquals(
        false,
        result.supported(),
        "Proxy must propagate the return value from the delegate unchanged");
  }

  @Test
  public void testProxyRestoresTcclAfterException() throws Exception {
    ClassLoader testClassLoader = Thread.currentThread().getContextClassLoader();
    when(delegate.caseSensitiveOnName(Capability.Scope.SCHEMA))
        .thenThrow(new RuntimeException("simulated failure"));

    Capability proxy =
        CapabilityHelpers.getCapability(
            NameIdentifier.of("metalake", "catalog", "schema"), catalogManager);
    assertThrows(RuntimeException.class, () -> proxy.caseSensitiveOnName(Capability.Scope.SCHEMA));

    assertSame(
        testClassLoader,
        Thread.currentThread().getContextClassLoader(),
        "TCCL must be restored even when the delegate throws an exception");
  }
}
