/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.hive;

import com.google.common.collect.ImmutableMap;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// Referred from Apache Iceberg's TestCachedClientPool implementation
// hive-metastore/src/test/java/org/apache/iceberg/hive/TestCachedClientPool.java
public class TestCachedClientPool extends MiniHiveMetastoreService {
  @Test
  public void testClientPoolCleaner() throws InterruptedException {
    Map<String, String> props =
        ImmutableMap.of(
            HiveConstants.CLIENT_POOL_SIZE,
            "1",
            HiveConstants.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            "5000");
    CachedClientPool clientPool = new CachedClientPool(MiniHiveMetastoreService.hiveConf, props);
    HiveClientPool clientPool1 = clientPool.clientPool();
    HiveClientPool cachedClientPool =
        clientPool.clientPoolCache().getIfPresent(CachedClientPool.extractKey());
    Assertions.assertSame(clientPool1, cachedClientPool);
    TimeUnit.MILLISECONDS.sleep(5000 - TimeUnit.SECONDS.toMillis(2));
    HiveClientPool clientPool2 = clientPool.clientPool();
    Assertions.assertSame(clientPool2, clientPool1);
    TimeUnit.MILLISECONDS.sleep(5000 + TimeUnit.SECONDS.toMillis(5));
    Assertions.assertNull(clientPool.clientPoolCache().getIfPresent(CachedClientPool.extractKey()));

    // The client has been really closed.
    Assertions.assertTrue(clientPool1.isClosed());
    Assertions.assertTrue(clientPool2.isClosed());
  }

  @Test
  public void testCacheKey() throws Exception {
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    UserGroupInformation foo1 = UserGroupInformation.createProxyUser("foo", current);
    UserGroupInformation foo2 = UserGroupInformation.createProxyUser("foo", current);
    UserGroupInformation bar = UserGroupInformation.createProxyUser("bar", current);
    CachedClientPool.Key key1 =
        foo1.doAs((PrivilegedAction<CachedClientPool.Key>) CachedClientPool::extractKey);
    CachedClientPool.Key key2 =
        foo2.doAs((PrivilegedAction<CachedClientPool.Key>) CachedClientPool::extractKey);
    CachedClientPool.Key key3 =
        bar.doAs((PrivilegedAction<CachedClientPool.Key>) CachedClientPool::extractKey);
    Assertions.assertEquals(key1, key2);
    Assertions.assertNotEquals(key1, key3);
  }
}
