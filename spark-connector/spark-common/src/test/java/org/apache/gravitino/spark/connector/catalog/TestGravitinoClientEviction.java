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

package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a Spark catalog keeps working after the manager's client cache evicts and closes
 * the Gravitino client it was initialized with.
 */
public class TestGravitinoClientEviction {

  private static final long CLIENT_TTL_SEC = 10;
  private static final String CATALOG_NAME = "test_catalog";

  private final FakeTicker ticker = new FakeTicker();
  private final List<FakeClient> clients = new ArrayList<>();

  @AfterEach
  void closeManager() {
    try {
      GravitinoCatalogManager.get().close();
    } catch (IllegalStateException e) {
      // The test closed the manager itself.
    }
  }

  @Test
  void testSparkCatalogKeepsWorkingAfterItsClientIsEvicted() throws Exception {
    GravitinoCatalogManager manager = createManager(client -> {});
    TestCatalog catalog = new TestCatalog();
    catalog.initialize(CATALOG_NAME, CaseInsensitiveStringMap.empty());
    assertArrayEquals(new String[][] {{"db"}}, catalog.listNamespaces());

    // Nothing touches the client for longer than its TTL, then another lookup evicts it.
    ticker.advance(CLIENT_TTL_SEC + 1);
    manager.getGravitinoCatalogInfo("other_catalog");
    assertTrue(await(() -> clients.get(0).closed.get()), "The idle client should be closed");

    assertArrayEquals(new String[][] {{"db"}}, catalog.listNamespaces());
    assertEquals(2, clients.size());
  }

  @Test
  void testCatalogInUseKeepsItsClient() throws Exception {
    GravitinoCatalogManager manager = createManager(client -> {});
    TestCatalog catalog = new TestCatalog();
    catalog.initialize(CATALOG_NAME, CaseInsensitiveStringMap.empty());

    // Each operation looks the catalog up, which counts as an access to its client.
    for (int i = 0; i < 3; i++) {
      ticker.advance(CLIENT_TTL_SEC - 1);
      catalog.listNamespaces();
    }
    manager.getGravitinoCatalogInfo("other_catalog");

    assertEquals(1, clients.size(), "A client in use must not expire");
    assertFalse(clients.get(0).closed.get());
  }

  @Test
  void testCatalogLoadedThroughAnEvictedClientIsLoadedAgain() {
    GravitinoIdentity identity = GravitinoIdentity.application("simple");
    AtomicBoolean evicted = new AtomicBoolean(false);
    GravitinoCatalogManager[] holder = new GravitinoCatalogManager[1];
    // While the first client is loading the catalog, the client cache evicts it.
    holder[0] =
        createManager(
            client ->
                client.onLoad =
                    () -> {
                      if (evicted.compareAndSet(false, true)) {
                        ticker.advance(CLIENT_TTL_SEC + 1);
                        holder[0].getClient(identity);
                      }
                    });
    GravitinoCatalogManager manager = holder[0];

    Catalog first = manager.getGravitinoCatalogInfo(identity, CATALOG_NAME);
    assertSame(clients.get(0).client, clientOf(first));

    Catalog second = manager.getGravitinoCatalogInfo(identity, CATALOG_NAME);
    assertSame(clients.get(1).client, clientOf(second));
  }

  private GravitinoCatalogManager createManager(Consumer<FakeClient> customizer) {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(
        GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_TTL_SEC, String.valueOf(CLIENT_TTL_SEC));
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_CATALOG_CACHE_TTL_SEC, "3600");
    return GravitinoCatalogManager.create(
        sparkConf,
        "spark-user",
        identity -> {
          FakeClient client = new FakeClient();
          customizer.accept(client);
          synchronized (clients) {
            clients.add(client);
          }
          return client.client;
        },
        ticker);
  }

  private GravitinoClient clientOf(Catalog catalog) {
    synchronized (clients) {
      for (FakeClient client : clients) {
        if (client.catalogs.contains(catalog)) {
          return client.client;
        }
      }
    }
    throw new AssertionError("Unknown catalog " + catalog);
  }

  private static boolean await(BooleanSupplier condition) throws InterruptedException {
    for (int i = 0; i < 100 && !condition.getAsBoolean(); i++) {
      Thread.sleep(50);
    }
    return condition.getAsBoolean();
  }

  /** A mock client whose catalogs fail like a shut-down connection pool once it is closed. */
  private static class FakeClient {
    private final GravitinoClient client = mock(GravitinoClient.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<Catalog> catalogs = new ArrayList<>();
    private Runnable onLoad = () -> {};

    private FakeClient() {
      when(client.loadCatalog(anyString())).thenAnswer(invocation -> newCatalog());
      doAnswer(
              invocation -> {
                closed.set(true);
                return null;
              })
          .when(client)
          .close();
    }

    private Catalog newCatalog() {
      onLoad.run();
      Catalog catalog = mock(Catalog.class);
      when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
      when(catalog.provider()).thenReturn("hive");
      when(catalog.supportsSecrets()).thenThrow(new UnsupportedOperationException());
      when(catalog.supportsCredentials()).thenThrow(new UnsupportedOperationException());
      SupportsSchemas schemas = mock(SupportsSchemas.class);
      when(schemas.listSchemas())
          .thenAnswer(
              invocation -> {
                if (closed.get()) {
                  throw new IllegalStateException("Connection pool shut down");
                }
                return new String[] {"db"};
              });
      when(catalog.asSchemas()).thenReturn(schemas);
      synchronized (catalogs) {
        catalogs.add(catalog);
      }
      return catalog;
    }
  }

  private static class FakeTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long read() {
      return nanos.get();
    }

    private void advance(long seconds) {
      nanos.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    }
  }

  private static class TestCatalog extends BaseCatalog {
    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      return mock(TableCatalog.class);
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return null;
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return null;
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return null;
    }
  }
}
