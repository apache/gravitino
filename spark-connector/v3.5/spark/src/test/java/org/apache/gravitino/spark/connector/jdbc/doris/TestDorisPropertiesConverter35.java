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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            "DBNAME",
            "createDatabaseIfNotExist",
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
            "spark.bypass.sslMode", "PREFERRED");
    Map<String, String> converted =
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(
                new CaseInsensitiveStringMap(
                    Map.of(
                        "fetchSize", "200",
                        "connectTimeout", "5000",
                        "sslMode", "VERIFY_IDENTITY")),
                properties);

    assertEquals("200", converted.get("fetchSize"));
    assertEquals("5000", converted.get("connectTimeout"));
    assertEquals("false", converted.get("pushDownPredicate"));
    assertEquals("VERIFY_IDENTITY", converted.get("sslMode"));
    assertFalse(converted.containsKey("connecttimeout"));
    assertFalse(converted.containsKey("sslmode"));
    assertFalse(converted.containsKey("fetchsize"));
    assertEquals(
        "100",
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(properties)
            .get("fetchSize"));
  }

  @Test
  void testNativeOptionsAreCatalogManagedAndNotPassedToJdbc() {
    Map<String, String> properties =
        Map.of(
            "jdbc-url", "jdbc:mysql://fe:9030/db",
            "spark.bypass.doris.fenodes", "fe:8030",
            "spark.bypass.doris.read.mode", "thrift",
            "spark.bypass.doris.request.tablet.size", "2",
            "spark.bypass.doris.batch.size", "1024",
            "spark.bypass.doris.read.arrow-flight-sql.port", "8070");
    Map<String, String> options = DorisPropertiesConverter35.nativeOptions(properties);
    assertEquals(
        Map.of(
            "doris.fenodes", "fe:8030",
            "doris.read.mode", "thrift",
            "doris.request.tablet.size", "2",
            "doris.batch.size", "1024",
            "doris.read.arrow-flight-sql.port", "8070"),
        options);
    Map<String, String> jdbc =
        DorisPropertiesConverter35.getInstance().toSparkCatalogProperties(properties);
    assertTrue(options.keySet().stream().noneMatch(jdbc::containsKey));
  }

  @Test
  void testNativeOptionsRejectUnmanagedConnectionAndQueryOverrides() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPropertiesConverter35.nativeOptions(
                Map.of("spark.bypass.doris.read.mode", "thrift")));
    assertInvalidNativeOption("doris.password", "secret");
    for (String name : List.of("doris.fenodes", "DORIS_password", "doris.table.identifier")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DorisPropertiesConverter35.validateNativeReadOptions(
                  new CaseInsensitiveStringMap(Map.of(name, "override"))));
    }
  }

  @Test
  void testJdbcOnlyCatalogPreservesExistingDorisPrefixedBypassOption() {
    Map<String, String> properties =
        Map.of(
            "jdbc-url", "jdbc:mysql://fe:9030/db",
            "spark.bypass.doris.custom", "legacy-value");

    assertTrue(DorisPropertiesConverter35.nativeOptions(properties).isEmpty());
    assertEquals(
        "legacy-value",
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(properties)
            .get("doris.custom"));
  }

  @Test
  void testNativeOptionsRejectInvalidTabletSizeAndReadMode() {
    for (String value : List.of("0", "-1", "bad", "2147483648")) {
      assertInvalidNativeOption("doris.request.tablet.size", value);
    }
    assertInvalidNativeOption("doris.read.mode", "other");
    for (String key : List.of("doris.batch.size", "doris.read.arrow-flight-sql.port")) {
      assertInvalidNativeOption(key, "65536");
    }
  }

  private static void assertInvalidNativeOption(String key, String value) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPropertiesConverter35.nativeOptions(
                Map.of("spark.bypass.doris.fenodes", "fe:8030", "spark.bypass." + key, value)));
  }
}
