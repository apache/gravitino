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

package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that BaseCatalog does not retain stale or closed Gravitino clients when cache entries
 * expire or are evicted due to size limits.
 */
public class TestBaseCatalogClientEviction {

  private static final String CATALOG_NAME = "test_catalog";

  private TestClientFactory clientFactory;

  @AfterEach
  void closeManager() {
    try {
      GravitinoCatalogManager.get().close();
    } catch (IllegalStateException e) {
      // The test closed the manager itself or it was not created.
    }
  }

  @Test
  void testOperationSucceedsAfterClientCacheExpiration() throws Exception {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_TTL_SEC, "1");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_CATALOG_CACHE_TTL_SEC, "1");

    createManager(sparkConf);

    DummyBaseCatalog catalog = new DummyBaseCatalog();
    catalog.initialize(CATALOG_NAME, CaseInsensitiveStringMap.empty());

    // First operation initializes and uses the first client
    Identifier[] tables1 = catalog.listTables(new String[] {"default"});
    assertNotNull(tables1);
    assertEquals(1, tables1.length);
    assertEquals(1, clientFactory.clientCount());

    // Wait for the client cache to expire and close the first client
    assertTrue(
        await(() -> clientFactory.closedCount() >= 1),
        "Client cache should expire and close the client");

    // Subsequent operation on the same BaseCatalog must succeed by re-resolving a fresh client
    Identifier[] tables2 = catalog.listTables(new String[] {"default"});
    assertNotNull(tables2);
    assertEquals(1, tables2.length);
    assertEquals(2, clientFactory.clientCount());
  }

  @Test
  void testOperationSucceedsAfterClientCacheSizeEviction() throws Exception {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.TOKEN_AUTH_TYPE);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_MAX_SIZE, "1");

    createManager(sparkConf);

    DummyBaseCatalog catalog = new DummyBaseCatalog();

    // Alice initializes catalog and performs operation
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice"));
    catalog.initialize(CATALOG_NAME, CaseInsensitiveStringMap.empty());

    Identifier[] aliceTables1 = catalog.listTables(new String[] {"default"});
    assertNotNull(aliceTables1);
    assertEquals(1, clientFactory.clientCount());

    // Bob accesses catalog, triggering size eviction of Alice's client
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("bob"));
    Identifier[] bobTables = catalog.listTables(new String[] {"default"});
    assertNotNull(bobTables);
    assertEquals(2, clientFactory.clientCount());

    assertTrue(
        await(() -> clientFactory.closedCount() >= 1),
        "Alice's client should be evicted and closed");

    // Alice accesses the same BaseCatalog instance again; must re-resolve a new client
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice"));
    Identifier[] aliceTables2 = catalog.listTables(new String[] {"default"});
    assertNotNull(aliceTables2);
    assertEquals(3, clientFactory.clientCount());
  }

  private void createManager(SparkConf sparkConf) {
    clientFactory = new TestClientFactory();
    GravitinoCatalogManager.create(sparkConf, "spark-user", clientFactory);
  }

  private static String jwt(String subject) {
    return base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}")
        + "."
        + base64Url(String.format("{\"sub\":\"%s\",\"jti\":\"test\"}", subject))
        + ".signature";
  }

  private static String base64Url(String value) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static boolean await(BooleanSupplier condition) {
    for (int i = 0; i < 100; i++) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }

  private static class DummyBaseCatalog extends BaseCatalog {

    @Override
    protected org.apache.spark.sql.connector.catalog.TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog =
          mock(org.apache.spark.sql.connector.catalog.TableCatalog.class);
      when(sparkCatalog.defaultNamespace()).thenReturn(new String[] {"default"});
      return sparkCatalog;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        org.apache.spark.sql.connector.catalog.TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return mock(Table.class);
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return mock(PropertiesConverter.class);
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return mock(SparkTransformConverter.class);
    }
  }

  /** ClientFactory that creates mock clients and fails if a closed client/catalog is invoked. */
  private static class TestClientFactory implements Function<GravitinoIdentity, GravitinoClient> {

    private final List<AtomicBoolean> closedFlags = new ArrayList<>();
    private final AtomicInteger clients = new AtomicInteger();

    @Override
    public GravitinoClient apply(GravitinoIdentity identity) {
      clients.incrementAndGet();
      AtomicBoolean isClosed = new AtomicBoolean(false);
      synchronized (closedFlags) {
        closedFlags.add(isClosed);
      }

      GravitinoClient client = mock(GravitinoClient.class);
      TableCatalog tableCatalog = mock(TableCatalog.class);
      Catalog catalog = mock(Catalog.class);

      when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
      when(catalog.provider()).thenReturn("hive");
      when(catalog.name()).thenReturn(CATALOG_NAME);
      when(catalog.asTableCatalog()).thenReturn(tableCatalog);

      when(tableCatalog.listTables(any(Namespace.class)))
          .thenAnswer(
              invocation -> {
                if (isClosed.get()) {
                  throw new IllegalStateException("Transport is closed: client has been evicted");
                }
                return new NameIdentifier[] {NameIdentifier.of("default", "table1")};
              });

      when(client.loadCatalog(anyString()))
          .thenAnswer(
              invocation -> {
                if (isClosed.get()) {
                  throw new IllegalStateException("Transport is closed: client has been evicted");
                }
                return catalog;
              });

      doAnswer(
              invocation -> {
                isClosed.set(true);
                return null;
              })
          .when(client)
          .close();

      return client;
    }

    int clientCount() {
      return clients.get();
    }

    int closedCount() {
      synchronized (closedFlags) {
        return (int) closedFlags.stream().filter(AtomicBoolean::get).count();
      }
    }
  }
}
