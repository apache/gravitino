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
package org.apache.gravitino.flink.connector.paimon;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link PaimonPropertiesConverter} */
public class TestPaimonPropertiesConverter {

  private static final PaimonPropertiesConverter CONVERTER = PaimonPropertiesConverter.INSTANCE;

  private static final String localWarehouse = "file:///tmp/paimon_warehouse";

  @Test
  public void testToPaimonFileSystemCatalog() {
    Map<String, String> catalogProperties = ImmutableMap.of("warehouse", localWarehouse);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(localWarehouse, flinkCatalogProperties.get("warehouse"));
  }

  @Test
  public void testToPaimonJdbcCatalog() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            PaimonConfig.CATALOG_WAREHOUSE.getKey(),
            localWarehouse,
            PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND,
            "jdbc",
            PaimonConfig.CATALOG_JDBC_USER.getKey(),
            testUser,
            PaimonConfig.CATALOG_JDBC_PASSWORD.getKey(),
            testPassword,
            PaimonConfig.CATALOG_URI.getKey(),
            testUri);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(
        localWarehouse,
        flinkCatalogProperties.get(GravitinoPaimonCatalogFactoryOptions.WAREHOUSE.key()));
    Assertions.assertEquals(
        testUser, flinkCatalogProperties.get(GravitinoPaimonCatalogFactoryOptions.JDBC_USER.key()));
    Assertions.assertEquals(
        testPassword,
        flinkCatalogProperties.get(GravitinoPaimonCatalogFactoryOptions.JDBC_PASSWORD.key()));
    Assertions.assertEquals(
        "jdbc",
        flinkCatalogProperties.get(GravitinoPaimonCatalogFactoryOptions.CATALOG_BACKEND.key()));
    Assertions.assertEquals(
        testUri, flinkCatalogProperties.get(GravitinoPaimonCatalogFactoryOptions.URI.key()));
  }

  @Test
  public void testToGravitinoCatalogProperties() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                GravitinoPaimonCatalogFactoryOptions.WAREHOUSE.key(),
                localWarehouse,
                GravitinoPaimonCatalogFactoryOptions.CATALOG_BACKEND.key(),
                "jdbc",
                GravitinoPaimonCatalogFactoryOptions.JDBC_USER.key(),
                testUser,
                GravitinoPaimonCatalogFactoryOptions.JDBC_PASSWORD.key(),
                testPassword,
                GravitinoPaimonCatalogFactoryOptions.URI.key(),
                testUri));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(
        localWarehouse, properties.get(PaimonConfig.CATALOG_WAREHOUSE.getKey()));
    Assertions.assertEquals(testUser, properties.get(PaimonConfig.CATALOG_JDBC_USER.getKey()));
    Assertions.assertEquals(
        testPassword, properties.get(PaimonConfig.CATALOG_JDBC_PASSWORD.getKey()));
    Assertions.assertEquals(testUri, properties.get(PaimonConfig.CATALOG_URI.getKey()));
  }
}
