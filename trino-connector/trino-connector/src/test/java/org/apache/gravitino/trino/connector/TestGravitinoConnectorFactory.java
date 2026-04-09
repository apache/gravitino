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
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.junit.jupiter.api.Test;

public class TestGravitinoConnectorFactory {

  /**
   * Regression test for #10717: starting the connector in multi-metalake mode against a Trino
   * version whose adapter reports {@code supportCatalogNameWithMetalake() == false} must not throw.
   * The split refactor in #9735 inadvertently re-introduced a hard error here, undoing the
   * warning-only behavior added by #7256.
   */
  @Test
  public void testCheckTrinoSpiVersionDoesNotThrowWhenMultiMetalakeOnUnsupportedVersion() {
    GravitinoConnectorFactory factory = newFactoryWithCatalogNameWithMetalakeUnsupported();
    ConnectorContext context = mockContext("478");
    GravitinoConfig config = newConfig(false);

    assertDoesNotThrow(() -> factory.checkTrinoSpiVersion(context, config));
  }

  /**
   * Sanity test: relaxing the catalog-name-with-metalake check must not relax the separate SPI
   * min/max version check, which still throws for SPI versions outside the supported window.
   */
  @Test
  public void testCheckTrinoSpiVersionStillThrowsForUnsupportedSpiVersion() {
    GravitinoConnectorFactory factory = newFactoryWithCatalogNameWithMetalakeUnsupported();
    ConnectorContext context = mockContext("100");
    GravitinoConfig config = newConfig(true);

    TrinoException error =
        assertThrows(TrinoException.class, () -> factory.checkTrinoSpiVersion(context, config));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION.toErrorCode(), error.getErrorCode());
  }

  /**
   * Sanity test: in single-metalake mode the catalog-name-with-metalake check is bypassed entirely
   * regardless of what the adapter reports.
   */
  @Test
  public void testCheckTrinoSpiVersionDoesNotWarnInSingleMetalakeMode() {
    GravitinoConnectorFactory factory = newFactoryWithCatalogNameWithMetalakeUnsupported();
    ConnectorContext context = mockContext("478");
    GravitinoConfig config = newConfig(true);

    assertDoesNotThrow(() -> factory.checkTrinoSpiVersion(context, config));
  }

  private static GravitinoConnectorFactory newFactoryWithCatalogNameWithMetalakeUnsupported() {
    return new GravitinoConnectorFactory(mock(GravitinoAdminClient.class)) {
      @Override
      protected boolean supportCatalogNameWithMetalake() {
        return false;
      }

      @Override
      protected int getMinSupportTrinoSpiVersion() {
        return 473;
      }

      @Override
      protected int getMaxSupportTrinoSpiVersion() {
        return 478;
      }
    };
  }

  private static ConnectorContext mockContext(String spiVersion) {
    ConnectorContext context = mock(ConnectorContext.class);
    when(context.getSpiVersion()).thenReturn(spiVersion);
    return context;
  }

  private static GravitinoConfig newConfig(boolean useSingleMetalake) {
    return new GravitinoConfig(
        ImmutableMap.of(
            "gravitino.uri",
            "http://127.0.0.1:8090",
            "gravitino.metalake",
            "test",
            "gravitino.use-single-metalake",
            Boolean.toString(useSingleMetalake)));
  }
}
