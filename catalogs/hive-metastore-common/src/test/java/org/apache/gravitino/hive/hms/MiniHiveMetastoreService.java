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
package org.apache.gravitino.hive.hms;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// hive-metastore/src/test/java/org/apache/iceberg/hive/HiveMetastoreTest.java
public abstract class MiniHiveMetastoreService {
  public static final Logger LOG = LoggerFactory.getLogger(MiniHiveMetastoreService.class);
  protected static final String DB_NAME = "hivedb";

  protected static HiveMetaStoreClient metastoreClient;

  protected static HiveConf hiveConf;
  protected static MiniHiveMetastore metastore;

  @BeforeAll
  public static void startMetastore() throws Exception {
    startMetastore(Collections.emptyMap());
  }

  public static void startMetastore(Map<String, String> hiveConfOverride) throws Exception {
    LOG.info("Starting Hive Metastore");
    MiniHiveMetastoreService.metastore = new MiniHiveMetastore();
    HiveConf hiveConfWithOverrides = new HiveConf(MiniHiveMetastore.class);
    if (hiveConfOverride != null) {
      for (Map.Entry<String, String> kv : hiveConfOverride.entrySet()) {
        hiveConfWithOverrides.set(kv.getKey(), kv.getValue());
      }
    }

    metastore.start(hiveConfWithOverrides);
    MiniHiveMetastoreService.hiveConf = metastore.hiveConf();
    MiniHiveMetastoreService.metastoreClient = new HiveMetaStoreClient(hiveConfWithOverrides);
    String dbPath = metastore.getDatabasePath(DB_NAME);
    Database db = new Database(DB_NAME, "description", dbPath, Maps.newHashMap());
    metastoreClient.createDatabase(db);
  }

  @AfterAll
  public static void stopMetastore() throws Exception {
    LOG.info("Stop Hive Metastore");
    metastoreClient.close();
    MiniHiveMetastoreService.metastoreClient = null;

    metastore.stop();
    MiniHiveMetastoreService.metastore = null;
  }

  protected static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
