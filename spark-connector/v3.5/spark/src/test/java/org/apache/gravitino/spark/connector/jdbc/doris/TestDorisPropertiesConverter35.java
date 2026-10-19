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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

/** Tests for the specialized Doris catalog property boundary. */
public class TestDorisPropertiesConverter35 {

  @Test
  void testCatalogPropertiesMapWithoutCatalogCredentials() {
    Map<String, String> properties =
        ImmutableMap.of(
            "jdbc-url", "jdbc:mysql://fe:9030/db",
            "jdbc-driver", "com.mysql.cj.jdbc.Driver",
            "jdbc-user", "catalog-user",
            "jdbc-password", "catalog-password");

    Map<String, String> converted =
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(new CaseInsensitiveStringMap(Map.of()), properties);

    assertEquals("jdbc:mysql://fe:9030/db", converted.get("url"));
    assertEquals("com.mysql.cj.jdbc.Driver", converted.get("driver"));
    assertFalse(converted.containsKey("user"));
    assertFalse(converted.containsKey("password"));
  }

  @Test
  void testSparkCatalogOptionsAreRejected() {
    for (String key :
        List.of(
            "URL",
            "user",
            "password",
            "driver",
            "jdbc-url",
            "jdbc-user",
            "jdbc-password",
            "jdbc-driver",
            "dbtable",
            "query",
            "prepareQuery",
            "sessionInitStatement")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DorisPropertiesConverter35.getInstance()
                  .toSparkCatalogProperties(
                      new CaseInsensitiveStringMap(Map.of(key, "override")),
                      Map.of("jdbc-url", "jdbc:mysql://fe:9030/db")),
          key);
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DorisPropertiesConverter35.getInstance()
                  .toSparkCatalogProperties(
                      Map.of(
                          "jdbc-url",
                          "jdbc:mysql://fe:9030/db",
                          "spark.bypass." + key,
                          "override")),
          "spark.bypass." + key);
    }
  }

  @Test
  void testSafeReadOptionsPreserveBypassDefaultsAndApplicationPrecedence() {
    Map<String, String> properties =
        Map.of(
            "jdbc-url", "jdbc:mysql://fe:9030/db",
            "jdbc-driver", "com.mysql.cj.jdbc.Driver",
            "spark.bypass.fetchSize", "100",
            "spark.bypass.pushDownPredicate", "false",
            "spark.bypass.sslMode", "VERIFY_IDENTITY");
    CaseInsensitiveStringMap converted =
        new CaseInsensitiveStringMap(
            DorisPropertiesConverter35.getInstance()
                .toSparkCatalogProperties(
                    new CaseInsensitiveStringMap(
                        Map.of("fetchSize", "200", "connectTimeout", "5000")),
                    properties));

    assertEquals("200", converted.get("fetchsize"));
    assertEquals("5000", converted.get("connectTimeout"));
    assertEquals("false", converted.get("pushDownPredicate"));
    assertEquals("VERIFY_IDENTITY", converted.get("sslMode"));
    assertEquals(
        "100",
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(properties)
            .get("fetchSize"));
  }
}
