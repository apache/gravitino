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

package com.datastrato.gravitino.catalog.hive;

import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.datastrato.gravitino.catalog.hive.miniHMS.MiniHiveMetastoreService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCachedClientPool extends MiniHiveMetastoreService {
    @Test
    public void testClientPoolCleaner() throws InterruptedException {
        CachedClientPool clientPool = new CachedClientPool(hiveConf, Collections.emptyMap());
        HiveClientPool clientPool1 = clientPool.clientPool();
        Assertions.assertEquals(clientPool1, )
                .isSameAs(
                        CachedClientPool.clientPoolCache()
                                .getIfPresent(CachedClientPool.extractKey(null, hiveConf)));
        TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL - TimeUnit.SECONDS.toMillis(2));
        HiveClientPool clientPool2 = clientPool.clientPool();
        assertThat(clientPool2).isSameAs(clientPool1);
        TimeUnit.MILLISECONDS.sleep(EVICTION_INTERVAL + TimeUnit.SECONDS.toMillis(5));
        assertThat(
                CachedClientPool.clientPoolCache()
                        .getIfPresent(CachedClientPool.extractKey(null, hiveConf)))
                .isNull();

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
                foo1.doAs(
                        (PrivilegedAction<CachedClientPool.Key>)
                                () -> CachedClientPool.extractKey("user_name,conf:key1", hiveConf));
        CachedClientPool.Key key2 =
                foo2.doAs(
                        (PrivilegedAction<CachedClientPool.Key>)
                                () -> CachedClientPool.extractKey("conf:key1,user_name", hiveConf));
        assertThat(key2).as("Key elements order shouldn't matter").isEqualTo(key1);

        key1 = foo1.doAs((PrivilegedAction<CachedClientPool.Key>) () -> CachedClientPool.extractKey("ugi", hiveConf));
        key2 = bar.doAs((PrivilegedAction<CachedClientPool.Key>) () -> CachedClientPool.extractKey("ugi", hiveConf));
        assertThat(key2).as("Different users are not supposed to be equivalent").isNotEqualTo(key1);

        key2 = foo2.doAs((PrivilegedAction<Key>) () -> CachedClientPool.extractKey("ugi", hiveConf));
        assertThat(key2)
                .as("Different UGI instances are not supposed to be equivalent")
                .isNotEqualTo(key1);

        key1 = CachedClientPool.extractKey("ugi", hiveConf);
        key2 = CachedClientPool.extractKey("ugi,conf:key1", hiveConf);
        assertThat(key2)
                .as("Keys with different number of elements are not supposed to be equivalent")
                .isNotEqualTo(key1);

        Configuration conf1 = new Configuration(hiveConf);
        Configuration conf2 = new Configuration(hiveConf);

        conf1.set("key1", "val");
        key1 = CachedClientPool.extractKey("conf:key1", conf1);
        key2 = CachedClientPool.extractKey("conf:key1", conf2);
        assertThat(key2)
                .as("Config with different values are not supposed to be equivalent")
                .isNotEqualTo(key1);

        conf2.set("key1", "val");
        conf2.set("key2", "val");
        key2 = CachedClientPool.extractKey("conf:key2", conf2);
        assertThat(key2)
                .as("Config with different keys are not supposed to be equivalent")
                .isNotEqualTo(key1);

        key1 = CachedClientPool.extractKey("conf:key1,ugi", conf1);
        key2 = CachedClientPool.extractKey("ugi,conf:key1", conf2);
        assertThat(key2).as("Config with same key/value should be equivalent").isEqualTo(key1);

        conf1.set("key2", "val");
        key1 = CachedClientPool.extractKey("conf:key2 ,conf:key1", conf1);
        key2 = CachedClientPool.extractKey("conf:key2,conf:key1", conf2);
        assertThat(key2).as("Config with same key/value should be equivalent").isEqualTo(key1);
        Assertions.assertEquals(key2, key1);

        Assertions.assertThrows(
                () -> CachedClientPool.extractKey("ugi,ugi", hiveConf),
                "Duplicate key elements should result in an error")
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("UGI key element already specified");

        Assertions.assertThrows(
                () -> CachedClientPool.extractKey("conf:k1,conf:k2,CONF:k1", hiveConf),
                "Duplicate conf key elements should result in an error")
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Conf key element k1 already specified");
    }

}
