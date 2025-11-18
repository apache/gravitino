/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.hive.client;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

class HiveShimV2 extends Shim {

  private HiveMetaStoreClient client;
  Method getAllDatabasesMethod;

  HiveShimV2(HiveMetaStoreClient client) {
    try {
      this.client = client;
      getAllDatabasesMethod = HiveMetaStoreClient.class.getMethod("getAllDatabases");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to initialize HiveShimV2", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<String> getAllDatabase() {
    try {
      return (List<String>) getAllDatabasesMethod.invoke(client);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get all databases using HiveShimV2", e);
    }
  }
}
