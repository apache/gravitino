/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.KerberosAwareIcebergCatalogProxy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that proxying a {@link CatalogWrapperForREST} does not run its constructor again, which
 * would create a second {@code CatalogCredentialManager}, scan plan cache and Hadoop configuration
 * that are never closed.
 */
public class TestKerberosAwareCatalogWrapperForREST {

  private static final AtomicInteger CONSTRUCTIONS = new AtomicInteger();
  private static final AtomicInteger CLOSES = new AtomicInteger();

  /** A wrapper that counts constructor runs and close calls. */
  public static class CountingRESTWrapper extends CatalogWrapperForREST {
    public CountingRESTWrapper(String catalogName, IcebergConfig config) {
      super(catalogName, config);
      CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
      CLOSES.incrementAndGet();
      super.close();
    }
  }

  @BeforeEach
  public void resetCounters() {
    CONSTRUCTIONS.set(0);
    CLOSES.set(0);
  }

  private static IcebergConfig memoryConfig() {
    return new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
  }

  @Test
  public void testProxyDoesNotRunWrapperConstructor() throws Exception {
    CountingRESTWrapper target = new CountingRESTWrapper("proxied", memoryConfig());
    IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

    Assertions.assertNotSame(target, proxy);
    Assertions.assertInstanceOf(CountingRESTWrapper.class, proxy);
    Assertions.assertEquals(1, CONSTRUCTIONS.get());
    // The proxy must not own its own credential manager and scan plan cache.
    Assertions.assertNull(((CatalogWrapperForREST) proxy).catalogCredentialManager);

    Assertions.assertSame(target.getCatalog(), proxy.getCatalog());
    Assertions.assertTrue(((CatalogWrapperForREST) proxy).supportsAsyncPurge());

    proxy.close();
    Assertions.assertEquals(1, CLOSES.get());
  }
}
