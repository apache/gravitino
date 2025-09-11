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
 *
 */
package org.apache.gravitino.iceberg.common.utils;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.hive.HiveClientPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergHiveCachedClientPool {

  @Test
  void test() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("hive.metastore.uris", "thrift://localhost:9083");
    Map<String, String> properties = Maps.newHashMap();
    IcebergHiveCachedClientPool clientPool =
        new IcebergHiveCachedClientPool(configuration, properties);

    // test extractKey for simple conf
    IcebergHiveCachedClientPool.Key key1 =
        IcebergHiveCachedClientPool.extractKey(null, configuration);
    IcebergHiveCachedClientPool.Key key2 =
        IcebergHiveCachedClientPool.extractKey(null, configuration);
    Assertions.assertEquals(key1, key2);

    // test clientPool
    HiveClientPool hiveClientPool1 = clientPool.clientPool();
    HiveClientPool hiveClientPool2 = clientPool.clientPool();
    Assertions.assertEquals(hiveClientPool1, hiveClientPool2);

    // test extractKey with user_name or ugi
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    UserGroupInformation foo1 = UserGroupInformation.createProxyUser("foo", current);
    UserGroupInformation foo2 = UserGroupInformation.createProxyUser("foo", current);
    UserGroupInformation bar = UserGroupInformation.createProxyUser("bar", current);

    IcebergHiveCachedClientPool.Key key3 =
        foo1.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("user_name", configuration));
    IcebergHiveCachedClientPool.Key key4 =
        foo2.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("user_name", configuration));
    Assertions.assertEquals(key3, key4);

    IcebergHiveCachedClientPool.Key key5 =
        foo1.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("user_name", configuration));
    IcebergHiveCachedClientPool.Key key6 =
        bar.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("user_name", configuration));
    Assertions.assertNotEquals(key5, key6);

    IcebergHiveCachedClientPool.Key key7 =
        foo1.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("ugi", configuration));
    IcebergHiveCachedClientPool.Key key8 =
        foo2.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("ugi", configuration));
    Assertions.assertNotEquals(key7, key8);

    // The equals method of UserGroupInformation: return this.subject ==
    // ((UserGroupInformation)o).subject;
    IcebergHiveCachedClientPool.Key key9 =
        foo1.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("ugi", configuration));
    IcebergHiveCachedClientPool.Key key10 =
        bar.doAs(
            (PrivilegedAction<IcebergHiveCachedClientPool.Key>)
                () -> IcebergHiveCachedClientPool.extractKey("ugi", configuration));
    Assertions.assertNotEquals(key9, key10);
  }

  @Test
  void testCloseMultipleInstances() throws IOException {
    Configuration configuration = new Configuration();
    Map<String, String> properties = Maps.newHashMap();
    IcebergHiveCachedClientPool pool1 = new IcebergHiveCachedClientPool(configuration, properties);
    IcebergHiveCachedClientPool pool2 = new IcebergHiveCachedClientPool(configuration, properties);

    Assertions.assertDoesNotThrow(
        () -> {
          pool1.close();
          pool2.close();
        });
  }
}
