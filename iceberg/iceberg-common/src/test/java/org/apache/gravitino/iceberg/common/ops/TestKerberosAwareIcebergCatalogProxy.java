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

package org.apache.gravitino.iceberg.common.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestKerberosAwareIcebergCatalogProxy {

  private static final AtomicInteger WRAPPER_CONSTRUCTIONS = new AtomicInteger();
  private static final AtomicInteger CATALOG_CONSTRUCTIONS = new AtomicInteger();
  private static final AtomicInteger CATALOG_CLOSES = new AtomicInteger();
  private static final AtomicInteger KERBEROS_OPERATIONS = new AtomicInteger();

  /** A catalog that counts how often it is created, closed and used under Kerberos. */
  public static class CountingKerberosCatalog extends InMemoryCatalog implements SupportsKerberos {
    public CountingKerberosCatalog() {
      CATALOG_CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public <R> R doKerberosOperations(Executable<R> executable) throws Throwable {
      KERBEROS_OPERATIONS.incrementAndGet();
      return executable.execute();
    }

    @Override
    public void close() {
      CATALOG_CLOSES.incrementAndGet();
    }
  }

  /** A wrapper that creates one {@link CountingKerberosCatalog} per constructed instance. */
  public static class CountingWrapper extends IcebergCatalogWrapper {
    private final Catalog countingCatalog = new CountingKerberosCatalog();

    public CountingWrapper(IcebergConfig config) {
      super(config);
      WRAPPER_CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public Catalog getCatalog() {
      return countingCatalog;
    }

    @Override
    public void close() throws Exception {
      ((AutoCloseable) countingCatalog).close();
    }
  }

  @BeforeEach
  public void resetCounters() {
    WRAPPER_CONSTRUCTIONS.set(0);
    CATALOG_CONSTRUCTIONS.set(0);
    CATALOG_CLOSES.set(0);
    KERBEROS_OPERATIONS.set(0);
  }

  private static IcebergConfig memoryConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(IcebergConfig.CATALOG_BACKEND.getKey(), "memory");
    return new IcebergConfig(conf);
  }

  @Test
  public void testProxyDoesNotRunWrapperConstructor() {
    CountingWrapper target = new CountingWrapper(memoryConfig());
    Assertions.assertEquals(1, WRAPPER_CONSTRUCTIONS.get());
    Assertions.assertEquals(1, CATALOG_CONSTRUCTIONS.get());

    IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

    Assertions.assertNotSame(target, proxy);
    Assertions.assertInstanceOf(CountingWrapper.class, proxy);
    Assertions.assertEquals(1, WRAPPER_CONSTRUCTIONS.get());
    Assertions.assertEquals(1, CATALOG_CONSTRUCTIONS.get());
  }

  @Test
  public void testProxyDelegatesToTargetCatalogUnderKerberos() {
    CountingWrapper target = new CountingWrapper(memoryConfig());
    IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

    Assertions.assertSame(target.getCatalog(), proxy.getCatalog());
    Assertions.assertTrue(KERBEROS_OPERATIONS.get() > 0);
  }

  @Test
  public void testCloseProxyClosesTargetCatalogOnce() throws Exception {
    CountingWrapper target = new CountingWrapper(memoryConfig());
    IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

    proxy.close();

    Assertions.assertEquals(1, CATALOG_CLOSES.get());
    Assertions.assertEquals(1, CATALOG_CONSTRUCTIONS.get());
  }
}
