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
package org.apache.gravitino.catalog.hive.integration.test;

import static org.apache.gravitino.catalog.hive.HiveConstants.DEFAULT_CATALOG;
import static org.apache.gravitino.catalog.hive.HiveConstants.METASTORE_URIS;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogHive3ITWithCatalog extends CatalogHive3IT {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogHive3ITWithCatalog.class);

  @Override
  protected void startNecessaryContainer() {
    super.startNecessaryContainer();
    hmsCatalog = "mycatalog";
    // spark does not support non-default catalog, disable spark test
    enableSparkTest = false;
  }

  @Override
  protected void createCatalog() {

    String location =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s/%s/%s",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            hmsCatalog,
            catalogName.toLowerCase(),
            schemaName.toLowerCase());
    try {
      hiveClientPool.run(
          client1 -> {
            client1.createCatalog(hmsCatalog, location);
            return null;
          });
    } catch (Exception e) {
      LOGGER.error("Error creating Hive catalog {}", hmsCatalog, e);
      throw new RuntimeException(e);
    }
    Map<String, String> properties = Maps.newHashMap();
    properties.put(METASTORE_URIS, hiveMetastoreUris);
    properties.put(DEFAULT_CATALOG, hmsCatalog);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);

    catalog = metalake.loadCatalog(catalogName);
  }
}
