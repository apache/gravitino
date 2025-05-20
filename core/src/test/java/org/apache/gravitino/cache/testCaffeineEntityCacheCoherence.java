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

package org.apache.gravitino.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.utils.TestUtil;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.L_Result;

public class testCaffeineEntityCacheCoherence {
  private static EntityStore mockStore;
  private static NameIdentifier ident1;
  private static NameIdentifier ident2;
  private static NameIdentifier ident3;
  private static NameIdentifier ident4;
  private static NameIdentifier ident5;
  private static NameIdentifier ident6;
  private static NameIdentifier ident7;

  // Test Entities
  private static SchemaEntity entity1;
  private static SchemaEntity entity2;
  private static TableEntity entity3;
  private static TableEntity entity4;
  private static TableEntity entity5;
  private static CatalogEntity entity6;
  private static BaseMetalake entity7;
  private static CaffeineEntityCache cache;

  static {
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
    ident4 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table2");
    ident5 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table3");
    ident6 = NameIdentifier.of("metalake1", "catalog1");
    ident7 = NameIdentifier.of("metalake1");

    entity1 =
        TestUtil.getTestSchemaEntity(
            1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");
    entity2 =
        TestUtil.getTestSchemaEntity(
            2L, "schema2", Namespace.of("metalake2", "catalog2"), "test_schema2");
    entity3 =
        TestUtil.getTestTableEntity(3L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));
    entity4 =
        TestUtil.getTestTableEntity(4L, "table2", Namespace.of("metalake1", "catalog2", "schema1"));
    entity5 =
        TestUtil.getTestTableEntity(5L, "table3", Namespace.of("metalake1", "catalog1", "schema2"));
    entity6 =
        TestUtil.getTestCatalogEntity(
            6L, "catalog1", Namespace.of("metalake1"), "hive", "test_catalog");
    entity7 = TestUtil.getTestMetalake(7L, "metalake1", "test_metalake1");

    mockStore = mock(EntityStore.class);

    try {
      when(mockStore.get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(entity1);
      when(mockStore.get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(entity2);

      when(mockStore.get(ident3, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity3);
      when(mockStore.get(ident4, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity4);
      when(mockStore.get(ident5, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity5);

      when(mockStore.get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class))
          .thenReturn(entity6);
      when(mockStore.get(ident7, Entity.EntityType.METALAKE, BaseMetalake.class))
          .thenReturn(entity7);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    cache = CaffeineEntityCache.getInstance(new CacheConfig(), mockStore);
  }

  @JCStressTest
  @Outcome(id = "ENTITY", expect = Expect.ACCEPTABLE, desc = "Both threads see correct value.")
  @Outcome(id = "NULL", expect = Expect.FORBIDDEN, desc = "Cache read failed unexpectedly")
  @State
  public static class GetOrLoadVsPutTest {
    @Actor
    public void actor1() {
      cache.put(entity1);
    }

    @Actor
    public void actor2(L_Result r) {
      Entity e = null;
      try {
        e = cache.getOrLoad(ident1, Entity.EntityType.SCHEMA);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      r.r1 = e != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome(id = "ENTITY", expect = Expect.ACCEPTABLE, desc = "Get sees the value after put.")
  @Outcome(id = "NULL", expect = Expect.FORBIDDEN, desc = "Put not visible to get.")
  @State
  public static class PutVsGetIfPresentTest {
    @Actor
    public void actor1() {
      cache.put(entity2);
    }

    @Actor
    public void actor2(L_Result r) {
      Entity e = cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).orElse(null);
      r.r1 = e != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome(id = "ENTITY", expect = Expect.ACCEPTABLE, desc = "GetOrLoad reloads after invalidate.")
  @Outcome(id = "NULL", expect = Expect.FORBIDDEN, desc = "Invalidate caused data loss.")
  @State
  public static class InvalidateVsGetOrLoadTest {

    public InvalidateVsGetOrLoadTest() {
      cache.put(entity3);
    }

    @Actor
    public void actor1() {
      cache.invalidate(ident3, Entity.EntityType.TABLE);
    }

    @Actor
    public void actor2(L_Result r) throws IOException {
      Entity e = cache.getOrLoad(ident3, Entity.EntityType.TABLE);
      r.r1 = (e != null) ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both puts succeeded; cache has 2 entries.")
  @Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "Only one put visible (timing issue).")
  @Outcome(
      id = "0",
      expect = Expect.FORBIDDEN,
      desc = "No put visible – broken write or visibility.")
  @State
  public static class ConcurrentPutDifferentKeysTest {
    @Actor
    public void actor1() {
      cache.put(entity4);
    }

    @Actor
    public void actor2() {
      cache.put(entity5);
    }

    @Arbiter
    public void arbiter(I_Result r) {
      int count = 0;
      if (cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent()) {
        count++;
      }
      if (cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent()) {
        count++;
      }
      r.r1 = count;
    }
  }

  @JCStressTest
  @Outcome(
      id = "ENTITY",
      expect = Expect.ACCEPTABLE,
      desc = "Put succeeded and survived invalidate.")
  @Outcome(
      id = "NULL",
      expect = Expect.ACCEPTABLE_INTERESTING,
      desc = "Invalidate cleared concurrent put.")
  @State
  public static class PutVsInvalidateTest {
    @Actor
    public void actor1() {
      cache.put(entity6);
    }

    @Actor
    public void actor2() {
      cache.invalidate(ident6, Entity.EntityType.CATALOG);
    }

    @Arbiter
    public void arbiter(L_Result r) {
      Entity result = cache.getIfPresent(ident6, Entity.EntityType.CATALOG).orElse(null);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome(
      id = "ENTITY",
      expect = Expect.ACCEPTABLE,
      desc = "Put happened after clear; value visible.")
  @Outcome(
      id = "NULL",
      expect = Expect.ACCEPTABLE_INTERESTING,
      desc = "Clear won; value not present.")
  @State
  public static class ClearVsPutTest {
    @Actor
    public void actor1() {
      cache.put(entity6);
    }

    @Actor
    public void actor2() {
      cache.clear();
    }

    @Arbiter
    public void arbiter(L_Result r) {
      Entity result = cache.getIfPresent(ident6, Entity.EntityType.CATALOG).orElse(null);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }

  @JCStressTest
  @Outcome(
      id = "ENTITY",
      expect = Expect.ACCEPTABLE,
      desc = "GetOrLoad loaded entity successfully.")
  @Outcome(id = "NULL", expect = Expect.FORBIDDEN, desc = "Load failed or cache broken.")
  @State
  public static class ClearVsGetOrLoadTest {
    @Actor
    public void actor1() {
      cache.clear();
    }

    @Actor
    public void actor2(L_Result r) throws IOException {
      Entity result = cache.getOrLoad(ident7, Entity.EntityType.METALAKE);
      r.r1 = result != null ? "ENTITY" : "NULL";
    }
  }
}
