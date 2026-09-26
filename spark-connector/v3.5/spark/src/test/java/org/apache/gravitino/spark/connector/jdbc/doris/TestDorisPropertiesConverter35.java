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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

/** Tests the protected Spark option contract of the Spark 3.5 Doris adapter. */
public class TestDorisPropertiesConverter35 {

  @Test
  void testCatalogPropertiesAndCredentialsAreMapped() {
    Map<String, String> properties =
        new HashMap<>(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_FE_NODES, "fe-1:8030,fe-2:8030",
                DorisConnectorConstants35.GRAVITINO_QUERY_PORT, "9030",
                DorisConnectorConstants35.JDBC_URL, "jdbc:mysql://fe-1:9030",
                DorisConnectorConstants35.JDBC_DRIVER, "com.mysql.cj.jdbc.Driver"));

    Map<String, String> options =
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of("doris.batch.size", "1000")),
                properties);

    assertEquals("fe-1:8030,fe-2:8030", options.get(DorisConnectorConstants35.DORIS_FE_NODES));
    assertEquals("9030", options.get(DorisConnectorConstants35.DORIS_QUERY_PORT));
    assertEquals("jdbc:mysql://fe-1:9030", options.get("url"));
    assertEquals("1000", options.get("doris.batch.size"));
  }

  @Test
  void testConfiguredJdbcCredentialsAreNotMappedWithoutVending() {
    String configuredPassword = "configured-" + UUID.randomUUID();
    Map<String, String> options =
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(
                ImmutableMap.of(
                    DorisConnectorConstants35.JDBC_URL,
                    "jdbc:mysql://fe-1:9030",
                    DorisConnectorConstants35.JDBC_DRIVER,
                    "com.mysql.cj.jdbc.Driver",
                    "jdbc-user",
                    "configured-user",
                    "jdbc-password",
                    configuredPassword));

    assertFalse(options.containsKey("user"));
    assertFalse(options.containsKey("password"));
    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoDorisCatalogSpark35.requireProperty(options, "user"));
    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoDorisCatalogSpark35.requireProperty(options, "password"));
  }

  @Test
  void testBypassAndApplicationOptionsPreserveMergeOrder() {
    Map<String, String> properties =
        ImmutableMap.of(
            "doris.batch.size", "100",
            "spark.bypass.doris.request.retries", "2");

    Map<String, String> options =
        DorisPropertiesConverter35.getInstance()
            .toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of("doris.batch.size", "1000")),
                properties);

    assertEquals("1000", options.get("doris.batch.size"));
    assertEquals("2", options.get("doris.request.retries"));
  }

  @Test
  void testProtectedAndUnknownOptionsAreRejected() {
    DorisPropertiesConverter35 converter = DorisPropertiesConverter35.getInstance();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of("doris.password", "secret")),
                ImmutableMap.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                new CaseInsensitiveStringMap(ImmutableMap.of("doris.unknown", "value")),
                ImmutableMap.of()));
  }

  @Test
  void testEndpointAndPortGrammarIsStrict() {
    DorisPropertiesConverter35 converter = DorisPropertiesConverter35.getInstance();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(DorisConnectorConstants35.GRAVITINO_FE_NODES, "http://fe:8030")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(DorisConnectorConstants35.GRAVITINO_FE_NODES, "fe:0")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toSparkCatalogProperties(
                ImmutableMap.of(DorisConnectorConstants35.GRAVITINO_QUERY_PORT, "65536")));
  }

  @Test
  void testPerWriteOptionsCannotOverrideGovernedPolicy() {
    assertDoesNotThrow(
        () ->
            DorisPropertiesConverter35.validateWriteOptions(
                new CaseInsensitiveStringMap(ImmutableMap.of("application.tag", "safe"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPropertiesConverter35.validateWriteOptions(
                new CaseInsensitiveStringMap(
                    ImmutableMap.of(DorisConnectorConstants35.DORIS_SINK_ENABLE_2PC, "false"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPropertiesConverter35.validateWriteOptions(
                new CaseInsensitiveStringMap(
                    ImmutableMap.of("spark.bypass.doris.sink.mode", "jdbc"))));
  }

  @Test
  void testPerReadOptionsCannotOverrideGovernedPolicy() {
    assertDoesNotThrow(
        () ->
            DorisPropertiesConverter35.validateReadOptions(
                new CaseInsensitiveStringMap(ImmutableMap.of())));
    for (String option : ImmutableList.of("url", "user", "password", "dbtable", "query")) {
      String secret = "secret-value-for-" + option;
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DorisPropertiesConverter35.validateReadOptions(
                      new CaseInsensitiveStringMap(ImmutableMap.of(option, secret))));
      assertFalse(failure.getMessage().contains(secret));
    }
  }

  @Test
  void testEveryProtectedWriteOptionIsRejectedWithoutLeakingValues() {
    DorisPropertiesConverter35 converter = DorisPropertiesConverter35.getInstance();
    for (String option :
        ImmutableList.of(
            DorisConnectorConstants35.DORIS_SINK_MODE,
            DorisConnectorConstants35.DORIS_SINK_AUTO_REDIRECT,
            DorisConnectorConstants35.DORIS_SINK_ENABLE_2PC,
            DorisConnectorConstants35.DORIS_SINK_STRICT_MODE,
            DorisConnectorConstants35.DORIS_MAX_FILTER_RATIO,
            DorisConnectorConstants35.DORIS_WRITE_SCHEMALESS,
            DorisConnectorConstants35.DORIS_FE_NODES,
            DorisConnectorConstants35.DORIS_QUERY_PORT,
            DorisConnectorConstants35.DORIS_USER,
            DorisConnectorConstants35.DORIS_PASSWORD,
            "url",
            "driver",
            "user",
            "password",
            "dbtable")) {
      String secret = "secret-value-for-" + option;
      IllegalArgumentException catalogFailure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  converter.toSparkCatalogProperties(
                      new CaseInsensitiveStringMap(ImmutableMap.of(option, secret)),
                      ImmutableMap.of()));
      assertFalse(catalogFailure.getMessage().contains(secret));

      IllegalArgumentException bypassFailure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  converter.toSparkCatalogProperties(
                      new CaseInsensitiveStringMap(ImmutableMap.of()),
                      ImmutableMap.of(PropertiesConverter.SPARK_PROPERTY_PREFIX + option, secret)));
      assertFalse(bypassFailure.getMessage().contains(secret));

      IllegalArgumentException writeFailure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DorisPropertiesConverter35.validateWriteOptions(
                      new CaseInsensitiveStringMap(ImmutableMap.of(option, secret))));
      assertFalse(writeFailure.getMessage().contains(secret));
    }
  }

  @Test
  void testConnectionAndCatalogKeysAreRejectedForWrites() {
    for (String option :
        ImmutableList.of(
            "jdbc-url",
            "jdbc-driver",
            "jdbc-user",
            "jdbc-password",
            "doris-fenodes",
            "doris-query-port",
            "query")) {
      String secret = "secret-value-for-" + option;
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DorisPropertiesConverter35.validateWriteOptions(
                      new CaseInsensitiveStringMap(ImmutableMap.of(option, secret))));
      assertFalse(failure.getMessage().contains(secret));
    }
  }

  @Test
  void testRequiredDorisPropertiesFailFast() {
    assertEquals(
        "fe:8030",
        GravitinoDorisCatalogSpark35.requireProperty(
            ImmutableMap.of(DorisConnectorConstants35.DORIS_FE_NODES, "fe:8030"),
            DorisConnectorConstants35.DORIS_FE_NODES));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoDorisCatalogSpark35.requireProperty(
                ImmutableMap.of(), DorisConnectorConstants35.DORIS_QUERY_PORT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoDorisCatalogSpark35.requireProperty(
                ImmutableMap.of(DorisConnectorConstants35.DORIS_FE_NODES, " "),
                DorisConnectorConstants35.DORIS_FE_NODES));
  }
}
