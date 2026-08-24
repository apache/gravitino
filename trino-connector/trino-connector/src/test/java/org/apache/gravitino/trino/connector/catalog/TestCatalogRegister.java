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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import org.junit.jupiter.api.Test;

public class TestCatalogRegister {

  private static final String DISCOVERED_ICEBERG_REST_URI_PROPERTY = "__gravitino.iceberg.rest-uri";

  @Test
  public void testGenerateCreateCatalogCommandEmbedsDiscoveredUriForIcebergCatalog()
      throws Exception {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", "test"));
    config.setDiscoveredIcebergRestUri("test", "http://irc-host:9001/iceberg");

    CatalogRegister catalogRegister = new CatalogRegister();
    catalogRegister.setConfigForTesting(config);

    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "iceberg_catalog",
            "lakehouse-iceberg",
            "test catalog",
            Catalog.Type.RELATIONAL,
            Collections.emptyMap());
    GravitinoCatalog catalog = new GravitinoCatalog("test", mockCatalog);

    String command = catalogRegister.generateCreateCatalogCommand("iceberg_catalog", catalog);

    assertTrue(
        command.contains(
            "\"" + DISCOVERED_ICEBERG_REST_URI_PROPERTY + "\":\"http://irc-host:9001/iceberg\""),
        "Expected the discovered Iceberg REST URI to be embedded in: " + command);
  }

  @Test
  public void testGenerateCreateCatalogCommandDoesNotEmbedUriForNonIcebergCatalog()
      throws Exception {
    // The discovered URI is per-Iceberg-catalog routing state; embedding it into every catalog's
    // properties (e.g. a Hive catalog) would be a leaky abstraction and is guarded against in
    // IcebergConnectorAdapter.embedDiscoveredIcebergRestUri. This asserts that guard actually
    // takes effect when reached through CatalogRegister, not just when called directly.
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.uri", "http://127.0.0.1:8090", "gravitino.metalake", "test"));
    config.setDiscoveredIcebergRestUri("test", "http://irc-host:9001/iceberg");

    CatalogRegister catalogRegister = new CatalogRegister();
    catalogRegister.setConfigForTesting(config);

    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "hive_catalog",
            "hive",
            "test catalog",
            Catalog.Type.RELATIONAL,
            Collections.emptyMap());
    GravitinoCatalog catalog = new GravitinoCatalog("test", mockCatalog);

    String command = catalogRegister.generateCreateCatalogCommand("hive_catalog", catalog);

    assertFalse(command.contains(DISCOVERED_ICEBERG_REST_URI_PROPERTY));
  }

  @Test
  public void testRedactSecretsMasksSecretBearingProperties() {
    String command =
        "CREATE CATALOG c USING gravitino WITH ( "
            + "\"gravitino.iceberg.rest-catalog.oauth2.credential\"='client:secretvalue', "
            + "\"gravitino.iceberg.rest-catalog.uri\"='http://irc-host:9001/iceberg', "
            + "\"some.token\"='abc123', "
            + "\"trino.bypass.password\"='hunter2')";

    String redacted = CatalogRegister.redactSecrets(command);

    assertFalse(redacted.contains("secretvalue"));
    assertFalse(redacted.contains("abc123"));
    assertFalse(redacted.contains("hunter2"));
    assertTrue(redacted.contains("\"gravitino.iceberg.rest-catalog.oauth2.credential\"='***'"));
    assertTrue(redacted.contains("\"some.token\"='***'"));
    assertTrue(redacted.contains("\"trino.bypass.password\"='***'"));
    // Non-secret properties must survive redaction unchanged.
    assertTrue(
        redacted.contains("\"gravitino.iceberg.rest-catalog.uri\"='http://irc-host:9001/iceberg'"));
  }
}
