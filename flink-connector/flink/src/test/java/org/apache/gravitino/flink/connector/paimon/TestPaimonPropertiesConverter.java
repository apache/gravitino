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
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.flink.connector.PropertiesConverter;
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
            PaimonConstants.WAREHOUSE,
            localWarehouse,
            PaimonConstants.CATALOG_BACKEND,
            "jdbc",
            PaimonConstants.GRAVITINO_JDBC_USER,
            testUser,
            PaimonConstants.GRAVITINO_JDBC_PASSWORD,
            testPassword,
            PropertiesConverter.FLINK_PROPERTY_PREFIX + PaimonConstants.URI,
            testUri);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(localWarehouse, flinkCatalogProperties.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals(testUser, flinkCatalogProperties.get(PaimonConstants.PAIMON_JDBC_USER));
    Assertions.assertEquals(
        testPassword, flinkCatalogProperties.get(PaimonConstants.PAIMON_JDBC_PASSWORD));
    Assertions.assertEquals("jdbc", flinkCatalogProperties.get(PaimonConstants.METASTORE));
    Assertions.assertEquals(testUri, flinkCatalogProperties.get(PaimonConstants.URI));
  }

  @Test
  public void testToGravitinoCatalogProperties() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    String testBackend = "jdbc";
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                PaimonConstants.WAREHOUSE,
                localWarehouse,
                PaimonConstants.METASTORE,
                testBackend,
                PaimonConstants.PAIMON_JDBC_USER,
                testUser,
                PaimonConstants.PAIMON_JDBC_PASSWORD,
                testPassword,
                PaimonConstants.URI,
                testUri));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(localWarehouse, properties.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals(testUser, properties.get(PaimonConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(testPassword, properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD));
    Assertions.assertEquals(testUri, properties.get(PaimonConstants.URI));
    Assertions.assertEquals(testBackend, properties.get(PaimonConstants.CATALOG_BACKEND));
  }
}
