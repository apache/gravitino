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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.Schema;
import org.junit.jupiter.api.Test;

public class TestHiveClient {

  @Test
  void testClient() throws Exception {
    Properties properties = new Properties();
    // properties.setProperty("hive.metastore.uris", "thrift://localhost:9083");
    properties.setProperty("hive.metastore.uris", "thrift://172.17.0.3:9083");
    IsolatedClientLoader loader = IsolatedClientLoader.forVersion("HIVE3", Map.of());
    HiveClient client = loader.createClient(properties);
    List<String> allDatabases = client.getAllDatabases();
    System.out.println("Databases: " + allDatabases);
    Schema db = client.getDatabase("hive", "default");
    System.out.println("Database s1: " + db.name());
  }
}
