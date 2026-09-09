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
package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableLocationProviderFactory {

  private static final Map<String, String> CATALOG_PROPERTIES =
      ImmutableMap.of("location", "/tmp/catalog");

  @Test
  void testCreateDefaultProvider() throws IOException {
    try (TableLocationProvider provider = create(DefaultTableLocationProvider.NAME)) {
      Assertions.assertInstanceOf(DefaultTableLocationProvider.class, provider);
      Assertions.assertEquals(DefaultTableLocationProvider.NAME, provider.name());
    }
  }

  @Test
  void testProviderNameIsCaseInsensitive() throws IOException {
    try (TableLocationProvider provider = create("DeFaUlT")) {
      Assertions.assertInstanceOf(DefaultTableLocationProvider.class, provider);
    }
  }

  @Test
  void testCreateCustomProviderDiscoveredViaServiceLoader() throws IOException {
    try (TableLocationProvider provider = create(FakeTableLocationProvider.NAME)) {
      FakeTableLocationProvider fake =
          Assertions.assertInstanceOf(FakeTableLocationProvider.class, provider);
      // The provider is initialized with the catalog properties before it is handed back.
      Assertions.assertEquals(CATALOG_PROPERTIES, fake.catalogProperties());
      Assertions.assertFalse(fake.isClosed());
    }
  }

  @Test
  void testEachCallReturnsANewInstance() throws IOException {
    try (TableLocationProvider first = create(DefaultTableLocationProvider.NAME);
        TableLocationProvider second = create(DefaultTableLocationProvider.NAME)) {
      Assertions.assertNotSame(first, second);
    }
  }

  @Test
  void testUnknownProviderThrows() {
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, () -> create("no-such-provider"));
    Assertions.assertTrue(
        e.getMessage().contains("No TableLocationProvider found for name 'no-such-provider'"),
        "Unexpected message: " + e.getMessage());
  }

  @Test
  void testBlankProviderNameThrows() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> create(" "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> create(null));
  }

  private static TableLocationProvider create(String name) {
    return TableLocationProviderFactory.create(name, CATALOG_PROPERTIES);
  }
}
