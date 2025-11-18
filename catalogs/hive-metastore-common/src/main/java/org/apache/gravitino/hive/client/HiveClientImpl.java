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
package org.apache.gravitino.hive.client;

import static org.apache.gravitino.hive.client.HiveClient.HiveVersion.HIVE3;

import java.util.List;
import org.apache.gravitino.Schema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

/**
 * Java version of HiveClientImpl from Spark Hive module. Provides full database, table, and
 * partition operations.
 */
public class HiveClientImpl implements HiveClient {

  Shim shim;

  public HiveClientImpl(HiveVersion hiveVersion, HiveMetaStoreClient hiveClient) {
    switch (hiveVersion) {
      case HIVE2:
        shim = new HiveShimV2(hiveClient);
        break;
      case HIVE3:
        shim = new HiveShimV3(hiveClient);
        break;
      default:
        throw new IllegalArgumentException("Unsupported Hive version: " + hiveVersion);
    }
  }

  @Override
  public List<String> getAllDatabases() {
    return shim.getAllDatabase();
  }

  @Override
  public void createDatabase(String catalogName, Schema database) {
    shim.createDatabase(catalogName, database);
  }

  @Override
  public Schema getDatabase(String catalogName, String dbName) {
    return shim.getDatabase(catalogName, dbName);
  }
}
