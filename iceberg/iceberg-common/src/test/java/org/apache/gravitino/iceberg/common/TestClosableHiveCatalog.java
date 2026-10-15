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

package org.apache.gravitino.iceberg.common;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.utils.IcebergHiveCachedClientPool;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hive.HiveClientPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link ClosableHiveCatalog} client-pool lifecycle across re-initialization. */
public class TestClosableHiveCatalog {

  /**
   * The Javadoc of {@code initialize} anticipates double initialization. Replacing the pool must
   * keep the shared static cache and its eviction scheduler intact: the replacement pool must
   * resolve the same cached {@link HiveClientPool} for the same key.
   */
  @Test
  public void testReinitializationKeepsTheSharedClientPool() throws Exception {
    ClosableHiveCatalog catalog = new ClosableHiveCatalog();
    catalog.initialize(
        "test_catalog", ImmutableMap.of(CatalogProperties.URI, "thrift://localhost:9083"));
    HiveClientPool cached = clientPoolOf(currentPool(catalog));

    catalog.initialize(
        "test_catalog", ImmutableMap.of(CatalogProperties.URI, "thrift://localhost:9083"));

    Assertions.assertSame(cached, clientPoolOf(currentPool(catalog)));
  }

  private static IcebergHiveCachedClientPool currentPool(ClosableHiveCatalog catalog)
      throws IllegalAccessException {
    return (IcebergHiveCachedClientPool) FieldUtils.readField(catalog, "clients", true);
  }

  private static HiveClientPool clientPoolOf(IcebergHiveCachedClientPool pool) throws Exception {
    java.lang.reflect.Method method =
        IcebergHiveCachedClientPool.class.getDeclaredMethod("clientPool");
    method.setAccessible(true);
    return (HiveClientPool) method.invoke(pool);
  }
}
