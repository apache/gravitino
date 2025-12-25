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

import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/** Hive3 HMS tests with an explicitly created catalog to validate catalog operations in Hive3. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHive3HMSWithCatalog extends TestHive3HMS {

  @BeforeAll
  @Override
  public void startHiveContainer() {
    testPrefix = "hive3_mycatalog";
    super.startHiveContainer();

    // Override catalog to a dedicated Hive3 catalog and ensure it exists.
    catalogName = "mycatalog";
    String catalogLocation =
        String.format(
            "hdfs://%s:%d/tmp/gravitino_test/catalogs/%s",
            hiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT, catalogName);

    // Ensure the catalog exists; only create if missing.
    if (!hiveClient.getCatalogs().contains(catalogName)) {
      hiveClient.createCatalog(catalogName, catalogLocation, "Hive3 catalog for tests");
    }
  }
}
