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
package org.apache.gravitino.catalog.doris;

import static org.apache.gravitino.catalog.doris.DorisCatalogPropertiesMetadata.DORIS_FE_NODES;
import static org.apache.gravitino.catalog.doris.DorisCatalogPropertiesMetadata.DORIS_QUERY_PORT;
import static org.apache.gravitino.catalog.jdbc.config.JdbcConfig.JDBC_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Test;

/** Tests for {@link DorisCatalogPropertiesMetadata}. */
public class TestDorisCatalogPropertiesMetadata {

  @Test
  void testDorisPropertiesExtendJdbcMetadata() {
    DorisCatalogPropertiesMetadata metadata = new DorisCatalogPropertiesMetadata();

    assertTrue(metadata.propertyEntries().containsKey(JDBC_URL.getKey()));
    assertTrue(metadata.propertyEntries().containsKey(DORIS_FE_NODES));
    assertTrue(metadata.propertyEntries().containsKey(DORIS_QUERY_PORT));
    PropertyEntry<?> queryPort = metadata.propertyEntries().get(DORIS_QUERY_PORT);
    assertEquals(Integer.class, queryPort.getJavaType());
    assertTrue(queryPort.isImmutable());
  }

  @Test
  void testTransformPropertiesPreservesJdbcAndNormalizesDorisProperties() {
    DorisCatalogPropertiesMetadata metadata = new DorisCatalogPropertiesMetadata();
    Map<String, String> properties =
        new HashMap<>(
            Map.of(
                JDBC_URL.getKey(),
                "jdbc:mysql://fe:9030",
                DORIS_FE_NODES,
                " fe-1:8030,fe-2:8030 ",
                DORIS_QUERY_PORT,
                "9030"));

    Map<String, String> transformed = metadata.transformProperties(properties);

    assertEquals("jdbc:mysql://fe:9030", transformed.get(JDBC_URL.getKey()));
    assertEquals("fe-1:8030,fe-2:8030", transformed.get(DORIS_FE_NODES));
    assertEquals("9030", transformed.get(DORIS_QUERY_PORT));
  }

  @Test
  void testInvalidDorisPropertiesFailClosed() {
    DorisCatalogPropertiesMetadata metadata = new DorisCatalogPropertiesMetadata();

    assertThrows(
        IllegalArgumentException.class,
        () -> metadata.transformProperties(Map.of(DORIS_FE_NODES, "http://fe:8030")));
    assertThrows(
        IllegalArgumentException.class,
        () -> metadata.transformProperties(Map.of(DORIS_FE_NODES, "fe:0")));
    assertThrows(
        IllegalArgumentException.class,
        () -> metadata.transformProperties(Map.of(DORIS_QUERY_PORT, "65536")));
  }
}
