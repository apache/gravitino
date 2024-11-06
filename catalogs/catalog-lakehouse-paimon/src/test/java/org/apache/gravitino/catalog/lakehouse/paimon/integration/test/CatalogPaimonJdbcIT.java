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
package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonJdbcIT extends CatalogPaimonBaseIT {

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "jdbc";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-paimon/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
    URI = mySQLContainer.getJdbcUrl(TEST_DB_NAME);
    jdbcUser = mySQLContainer.getUsername();
    jdbcPassword = mySQLContainer.getPassword();

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.URI, URI);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER, jdbcUser);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD, jdbcPassword);
    catalogProperties.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver");

    return catalogProperties;
  }
}
