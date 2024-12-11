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
            "warehouse",
            localWarehouse,
            GravitinoPaimonCatalogFactoryOptions.backendType.key(),
            "jdbc",
            "jdbc-user",
            testUser,
            "jdbc-password",
            testPassword,
            "uri",
            testUri);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(localWarehouse, flinkCatalogProperties.get("warehouse"));
    Assertions.assertEquals(testUser, flinkCatalogProperties.get("jdbc.user"));
    Assertions.assertEquals(testPassword, flinkCatalogProperties.get("jdbc.password"));
    Assertions.assertEquals("jdbc", flinkCatalogProperties.get("metastore"));
    Assertions.assertEquals(testUri, flinkCatalogProperties.get("uri"));
  }
}
