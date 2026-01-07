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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.junit.jupiter.api.Test;

public class TestCatalogRegisterTrinoVersionValidation {

  @Test
  public void testSupportedVersion() {
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "test"));
    assertDoesNotThrow(() -> CatalogRegister.validateTrinoSpiVersion("478", config));
  }

  @Test
  public void testUnsupportedVersion() {
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "test"));
    TrinoException e =
        assertThrows(
            TrinoException.class, () -> CatalogRegister.validateTrinoSpiVersion("435", config));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testSkipValidation() {
    GravitinoConfig config =
        new GravitinoConfig(
            ImmutableMap.of(
                "gravitino.metalake", "test", "gravitino.trino.skip-version-validation", "true"));
    assertDoesNotThrow(() -> CatalogRegister.validateTrinoSpiVersion("435", config));
  }

  @Test
  public void testInvalidSpiVersion() {
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "test"));
    TrinoException e =
        assertThrows(
            TrinoException.class, () -> CatalogRegister.validateTrinoSpiVersion("invalid", config));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION.toErrorCode(), e.getErrorCode());
  }
}
