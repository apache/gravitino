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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos.Executable;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper.IcebergTableChange;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

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
    private Catalog countingCatalog;

    public CountingWrapper(IcebergConfig config) {
      super(config);
      WRAPPER_CONSTRUCTIONS.incrementAndGet();
    }

    @Override
    public synchronized Catalog getCatalog() {
      // Load lazily like the real wrapper, so the catalog is only created on first access.
      if (countingCatalog == null) {
        countingCatalog = new CountingKerberosCatalog();
      }
      return countingCatalog;
    }

    @Override
    public void close() throws Exception {
      ((AutoCloseable) getCatalog()).close();
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
    Assertions.assertEquals(0, CATALOG_CONSTRUCTIONS.get());

    IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

    Assertions.assertNotSame(target, proxy);
    Assertions.assertInstanceOf(CountingWrapper.class, proxy);
    Assertions.assertEquals(1, WRAPPER_CONSTRUCTIONS.get());
    Assertions.assertEquals(1, CATALOG_CONSTRUCTIONS.get());
    // Catalog initialization bootstraps authentication before any proxied operation can use it.
    Assertions.assertEquals(0, KERBEROS_OPERATIONS.get());

    Assertions.assertSame(target.getCatalog(), proxy.getCatalog());
    Assertions.assertEquals(1, CATALOG_CONSTRUCTIONS.get());
    Assertions.assertEquals(1, KERBEROS_OPERATIONS.get());
  }

  @Test
  public void testProxyDelegatesToTargetCatalogUnderKerberos() {
    CountingWrapper target = new CountingWrapper(memoryConfig());
    IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

    Assertions.assertSame(target.getCatalog(), proxy.getCatalog());
    Assertions.assertTrue(KERBEROS_OPERATIONS.get() > 0);
  }

  @Test
  void testBuildAndUpdateTableRunsInsideKerberosContext() throws Throwable {
    Catalog catalog =
        Mockito.mock(Catalog.class, Mockito.withSettings().extraInterfaces(SupportsKerberos.class));
    AtomicBoolean insideKerberos = new AtomicBoolean();
    Mockito.doAnswer(
            invocation -> {
              Assertions.assertFalse(insideKerberos.getAndSet(true));
              try {
                return invocation.<Executable<?>>getArgument(0).execute();
              } finally {
                insideKerberos.set(false);
              }
            })
        .when((SupportsKerberos) catalog)
        .doKerberosOperations(Mockito.any());

    TableIdentifier identifier = TableIdentifier.of("db", "table");
    Table table = Mockito.mock(Table.class);
    Transaction transaction = Mockito.mock(Transaction.class);
    LoadTableResponse response = Mockito.mock(LoadTableResponse.class);
    Mockito.when(catalog.loadTable(identifier))
        .thenAnswer(
            invocation -> {
              Assertions.assertTrue(insideKerberos.get(), "Metadata must load under Kerberos");
              return table;
            });
    Mockito.when(table.newTransaction())
        .thenAnswer(
            invocation -> {
              Assertions.assertTrue(insideKerberos.get(), "Transaction must start under Kerberos");
              return transaction;
            });
    Mockito.doAnswer(
            invocation -> {
              Assertions.assertTrue(insideKerberos.get(), "Transaction must commit under Kerberos");
              return null;
            })
        .when(transaction)
        .commitTransaction();

    try (IcebergCatalogWrapper target = Mockito.spy(new IcebergCatalogWrapper(memoryConfig()))) {
      Mockito.doReturn(catalog).when(target).getCatalog();
      Mockito.doAnswer(
              invocation -> {
                Assertions.assertTrue(insideKerberos.get(), "Response must reload under Kerberos");
                return response;
              })
          .when(target)
          .loadTable(identifier);
      IcebergCatalogWrapper proxy = new KerberosAwareIcebergCatalogProxy(target).getProxy();

      LoadTableResponse actual =
          proxy.buildAndUpdateTable(
              builderCatalog -> {
                Assertions.assertTrue(insideKerberos.get(), "Builder must run under Kerberos");
                Assertions.assertSame(catalog, builderCatalog);
                return new IcebergTableChange(
                    identifier, builderCatalog.loadTable(identifier).newTransaction());
              });

      Assertions.assertSame(response, actual);
      Assertions.assertFalse(insideKerberos.get());
      Mockito.verify((SupportsKerberos) catalog).doKerberosOperations(Mockito.any());
      InOrder order = Mockito.inOrder(catalog, table, transaction, target);
      order.verify(catalog).loadTable(identifier);
      order.verify(table).newTransaction();
      order.verify(transaction).commitTransaction();
      order.verify(target).loadTable(identifier);
    }
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
